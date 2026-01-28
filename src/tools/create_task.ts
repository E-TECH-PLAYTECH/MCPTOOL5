import { z } from "zod";
import { v5 as uuidv5 } from "uuid";
import { buildEnvelope } from "../core/audit/envelope.js";
import { canonicalize, computeHash } from "../core/audit/canonical.js";
import { store } from "../core/store/db.js";

const TASK_NAMESPACE = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

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
  version: "1.0.1",

  execute: async (input: z.infer<typeof CreateTaskInputSchema>) => {
    const { task, mode, reference_time, idempotency_key } = input;

    // Deterministic schedule resolution
    let next_run_at: string;
    if (task.schedule.run_at) {
      next_run_at = task.schedule.run_at;
    } else if (task.schedule.interval_seconds) {
      if (!reference_time) {
        return buildEnvelope({
          request_id: input.request_id,
          tool_name: "create_task",
          tool_version: "1.0.1",
          input,
          result: null,
          errors: [{
            code: "ERR_DETERMINISM",
            message: "reference_time is required when using interval_seconds",
            path: "reference_time"
          }]
        });
      }
      const base = new Date(reference_time).getTime();
      const target = base + task.schedule.interval_seconds * 1000;
      next_run_at = new Date(target).toISOString();
    } else {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "create_task",
        tool_version: "1.0.1",
        input,
        result: null,
        errors: [{ code: "ERR_INVALID_SCHEDULE", message: "Either run_at or interval_seconds is required." }]
      });
    }

    // Normalize task deterministically
    const normalized_task = {
      title: task.title.trim(),
      action: task.action.toLowerCase().trim(),
      payload: task.payload,
      status: "pending",
      schedule: { next_run_at }
    };

    // Deterministic task_id
    const seed =
      mode === "commit"
        ? (idempotency_key ?? "")
        : computeHash(normalized_task);

    if (mode === "commit" && !idempotency_key) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "create_task",
        tool_version: "1.0.1",
        input,
        result: null,
        errors: [{ code: "ERR_IDEMPOTENCY_REQUIRED", message: "idempotency_key is required for commit mode", path: "idempotency_key" }]
      });
    }

    const task_id = uuidv5(seed, TASK_NAMESPACE);
    const payload_json = canonicalize(normalized_task.payload);

    if (mode === "dry_run") {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "create_task",
        tool_version: "1.0.1",
        input,
        result: {
          would_create: true,
          task_id,
          normalized_task
        }
      });
    }

    // commit mode (SQLite-backed, idempotent)
    const db = store.db;

    const tx = db.transaction(() => {
      const existing = db.prepare(`SELECT * FROM tasks WHERE task_id = ?`).get(task_id) as any;
      if (existing) {
        return {
          status: "idempotent_hit" as const,
          task_id,
          normalized_task: {
            title: existing.title,
            action: existing.action,
            payload: JSON.parse(existing.payload_json),
            status: existing.status,
            schedule: { next_run_at: existing.next_run_at }
          }
        };
      }

      db.prepare(`
        INSERT INTO tasks(task_id, idempotency_key, title, action, payload_json, status, next_run_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
      `).run(
        task_id,
        idempotency_key!,
        normalized_task.title,
        normalized_task.action,
        payload_json,
        normalized_task.status,
        next_run_at
      );

      return { status: "created" as const, task_id, normalized_task };
    });

    const out = tx();

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "create_task",
      tool_version: "1.0.1",
      input,
      result: out,
      provenance: [{ source_type: "db", source_id: "tasks", artifact_id: task_id }]
    });
  }
};
