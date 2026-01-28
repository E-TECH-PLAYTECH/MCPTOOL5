import { createHash } from "crypto";
import { zodToJsonSchema } from "zod-to-json-schema";
import { store } from "./store/db.js";
import { git } from "./store/git.js";
import { canonicalize } from "./audit/canonical.js";

import { RetrieveTool, RetrieveInputSchema } from "../tools/retrieve.js";
import { CommitIndexTool, CommitIndexInputSchema } from "../tools/commit_index.js";
import { LogIndexTool, LogIndexInputSchema } from "../tools/log_index.js";
import { DiffIndexTool, DiffIndexInputSchema } from "../tools/diff_index.js";
import { ValidateSchemaTool, ValidateSchemaInputSchema } from "../tools/validate_schema.js";
import { CheckSupportTool, CheckSupportInputSchema } from "../tools/check_support.js";
import { RedactTool, RedactInputSchema } from "../tools/redact.js";
import { CreateTaskTool, CreateTaskInputSchema } from "../tools/create_task.js";

const SYSTEM_TOOLS = [
  { def: RetrieveTool, schema: RetrieveInputSchema, desc: "Deterministic keyword retrieval over indexed chunks." },
  { def: CommitIndexTool, schema: CommitIndexInputSchema, desc: "Explicitly commit the current working tree into a deterministic DAG." },
  { def: LogIndexTool, schema: LogIndexInputSchema, desc: "Walk first-parent history from a ref or commit hash." },
  { def: DiffIndexTool, schema: DiffIndexInputSchema, desc: "Diff two committed trees (added/removed/changed doc_ids)." },
  { def: ValidateSchemaTool, schema: ValidateSchemaInputSchema, desc: "Validate a JSON payload against a file-backed JSON schema." },
  { def: CheckSupportTool, schema: CheckSupportInputSchema, desc: "Deterministic heuristic support checker (token overlap + number consistency)." },
  { def: RedactTool, schema: RedactInputSchema, desc: "File-backed redact policy engine (regex-based) with deterministic mapping." },
  { def: CreateTaskTool, schema: CreateTaskInputSchema, desc: "Deterministic task creation stored in SQLite (idempotent, reference_time aware)." }
];

export function indexSystemTools() {
  const db = store.db;

  const EPOCH = "1970-01-01T00:00:00.000Z";

  const tx = db.transaction(() => {
    // 1) Remove existing sys:tool rows (FTS discipline)
    const existing = db
      .prepare(`SELECT rowid, doc_id, text FROM chunks WHERE doc_id LIKE 'sys:tool:%'`)
      .all() as Array<{ rowid: number; doc_id: string; text: string }>;

    const deleteFTS = db.prepare(`INSERT INTO chunks_fts(chunks_fts, rowid, text) VALUES('delete', ?, ?)`);
    const deleteChunk = db.prepare(`DELETE FROM chunks WHERE doc_id = ?`);
    const deleteDoc = db.prepare(`DELETE FROM documents WHERE doc_id = ?`);

    for (const row of existing) {
      deleteFTS.run(row.rowid, row.text);
      deleteChunk.run(row.doc_id);
      deleteDoc.run(row.doc_id);
    }

    // 2) Insert new tool manifests
    const insertDoc = db.prepare(`
      INSERT INTO documents(doc_id, content_hash, title, updated_at)
      VALUES(?, ?, ?, ?)
    `);

    const insertChunk = db.prepare(`
      INSERT INTO chunks(chunk_id, doc_id, text, span_start, span_end, content_hash)
      VALUES(?, ?, ?, 0, length(?), ?)
    `);

    const insertFTS = db.prepare(`
      INSERT INTO chunks_fts(rowid, text) VALUES(?, ?)
    `);

    for (const t of SYSTEM_TOOLS) {
      const docId = `sys:tool:${t.def.name}`;
      const schemaJson = zodToJsonSchema(t.schema, t.def.name);

      const manifestObj = {
        tool: t.def.name,
        version: t.def.version,
        type: "System Capability",
        description: t.desc,
        schema: schemaJson
      };

      const manifestText = canonicalize(manifestObj);
      const hash = createHash("sha256").update(manifestText).digest("hex");

      insertDoc.run(docId, hash, `Tool: ${t.def.name}`, EPOCH);
      const info = insertChunk.run(`${docId}#def`, docId, manifestText, manifestText, hash);
      insertFTS.run((info as any).lastInsertRowid, manifestText);
    }

    // 3) Idempotent auto-commit (only if changed)
    const { treeHash: currentTree, entriesJson } = git.createTreeFromCurrentState(db);
    const head = git.getRef("HEAD", db);

    if (head) {
      const headTree = git.getTreeHashForCommit(head, db);
      if (headTree === currentTree) {
        return;
      }
    }

    git.saveTree(currentTree, entriesJson, db);
    const parents = head ? [head] : [];
    const commitHash = git.createCommit(currentTree, parents, "System Bootstrap: Tool Registry Sync", db);
    git.updateRef("main", commitHash, db);
    git.updateRef("HEAD", commitHash, db);
  });

  tx();
}
