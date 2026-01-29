import { z } from "zod";
import { v5 as uuidv5 } from "uuid";
import { store } from "../core/store/db.js";
import { buildEnvelope } from "../core/audit/envelope.js";
import { canonicalize, computeHash } from "../core/audit/canonical.js";

const TASK_NAMESPACE = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const EPOCH = "1970-01-01T00:00:00.000Z";

export const CreateTaskInputSchema = z.object({
  request_id: z.string().optional(),
  reference_time: z.string().datetime().optional(),
  task: z.object({
    title: z.string().min(1),
    action: z.string().min(1),
    payload: z.record(z.unknown()),
    schedule: z.object({
      run_at: z.string().datetime().optional(),
      interval_seconds: z.number().int().positive().optional()
    })
  }),
  mode: z.enum(["dry_run", "commit"]),
  idempotency_key: z.string().optional()
});

export const CreateTaskTool = {
  name: "create_task",
  version: "2.0.0",

  execute: async (input: z.infer<typeof CreateTaskInputSchema>) => {
    const { task, mode, reference_time, idempotency_key } = input;

    let next_run_at: string;
    if (task.schedule.run_at) next_run_at = task.schedule.run_at;
    else if (task.schedule.interval_seconds) {
      if (!reference_time) {
        return buildEnvelope({
          request_id: input.request_id,
          tool_name: "create_task",
          tool_version: "2.0.0",
          input,
          result: null,
          errors: [{ code: "ERR_DETERMINISM", message: "reference_time required for interval scheduling", path: "reference_time" }]
        });
      }
      const base = new Date(reference_time).getTime();
      next_run_at = new Date(base + task.schedule.interval_seconds * 1000).toISOString();
    } else {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "create_task",
        tool_version: "2.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_INVALID_SCHEDULE", message: "run_at or interval_seconds required", path: "task.schedule" }]
      });
    }

    const created_at = reference_time ?? task.schedule.run_at ?? EPOCH;

    const normalized_task = {
      title: task.title.trim(),
      action: task.action.toLowerCase().trim(),
      payload: task.payload,
      schedule: { next_run_at }
    };

    const task_json = canonicalize(normalized_task);

    const task_id =
      mode === "commit"
        ? (idempotency_key ? uuidv5(idempotency_key, TASK_NAMESPACE) : "")
        : uuidv5(computeHash(normalized_task), TASK_NAMESPACE);

    if (mode === "commit" && !idempotency_key) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "create_task",
        tool_version: "2.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_IDEMPOTENCY_REQUIRED", message: "idempotency_key required for commit mode", path: "idempotency_key" }]
      });
    }

    if (mode === "dry_run") {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "create_task",
        tool_version: "2.0.0",
        input,
        result: { would_create: true, task_id, normalized_task, created_at, next_run_at }
      });
    }

    const db = store.db;

    const tx = db.transaction(() => {
      const existing = db.prepare(`SELECT task_id, task_json, status, next_run_at, created_at FROM tasks WHERE task_id = ?`).get(task_id) as any;
      if (existing) {
        return {
          status: "idempotent_hit" as const,
          task_id: existing.task_id as string,
          normalized_task: JSON.parse(existing.task_json),
          created_at: existing.created_at as string,
          next_run_at: existing.next_run_at as string
        };
      }

      db.prepare(`
        INSERT INTO tasks(task_id, idempotency_key, task_json, status, next_run_at, created_at)
        VALUES(?, ?, ?, 'pending', ?, ?)
      `).run(task_id, idempotency_key!, task_json, next_run_at, created_at);

      return {
        status: "created" as const,
        task_id,
        normalized_task,
        created_at,
        next_run_at
      };
    });

    const out = tx();

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "create_task",
      tool_version: "2.0.0",
      input,
      result: out,
      provenance: [{ source_type: "db", source_id: "tasks", artifact_id: out.task_id }]
    });
  }
};
