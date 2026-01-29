import { createHash } from "crypto";
import { zodToJsonSchema } from "zod-to-json-schema";
import { store } from "./store/db.js";
import { git } from "./store/git.js";
import { canonicalize } from "./audit/canonical.js";

import { RetrieveTool, RetrieveInputSchema } from "../tools/retrieve.js";
import { CommitIndexTool, CommitIndexInputSchema } from "../tools/commit_index.js";
import { LogIndexTool, LogIndexInputSchema } from "../tools/log_index.js";
import { DiffIndexTool, DiffIndexInputSchema } from "../tools/diff_index.js";
import { CreateTaskTool, CreateTaskInputSchema } from "../tools/create_task.js";
import { BuildEmbeddingsTool, BuildEmbeddingsInputSchema } from "../tools/build_embeddings.js";
import { RetrieveWithEmbeddingsTool, RetrieveWithEmbeddingsInputSchema } from "../tools/retrieve_with_embeddings.js";
import { GcArtifactsTool, GcArtifactsInputSchema } from "../tools/gc_artifacts.js";
import { CheckoutIndexTool, CheckoutIndexInputSchema } from "../tools/checkout_index.js";

const SYSTEM_TOOLS = [
  { def: RetrieveTool, schema: RetrieveInputSchema, desc: "BM25 keyword search over working tree chunks." },
  { def: RetrieveWithEmbeddingsTool, schema: RetrieveWithEmbeddingsInputSchema, desc: "Hybrid BM25 + vector search over a committed tree." },
  { def: CommitIndexTool, schema: CommitIndexInputSchema, desc: "Commit working tree into a deterministic DAG (tree/commit/refs)." },
  { def: LogIndexTool, schema: LogIndexInputSchema, desc: "Deterministic commit history walk." },
  { def: DiffIndexTool, schema: DiffIndexInputSchema, desc: "Deterministic diff of two commits (added/removed/changed doc IDs)." },
  { def: BuildEmbeddingsTool, schema: BuildEmbeddingsInputSchema, desc: "Build and store embeddings for a committed tree." },
  { def: GcArtifactsTool, schema: GcArtifactsInputSchema, desc: "Garbage collect derived artifacts not reachable from refs." },
  { def: CheckoutIndexTool, schema: CheckoutIndexInputSchema, desc: "Checkout a committed tree into the working state." },
  { def: CreateTaskTool, schema: CreateTaskInputSchema, desc: "Deterministic durable task creation." }
];

export function indexSystemTools() {
  const db = store.db;

  const tx = db.transaction(() => {
    // Remove previous sys:tool entries (and clean FTS)
    const existing = db.prepare(`
      SELECT rowid, doc_id, text, content_hash
      FROM chunks
      WHERE doc_id LIKE 'sys:tool:%'
    `).all() as Array<any>;

    const deleteFTS = db.prepare(`INSERT INTO chunks_fts(chunks_fts, rowid, text) VALUES('delete', ?, ?)`);
    const deleteChunk = db.prepare(`DELETE FROM chunks WHERE doc_id = ?`);
    const deleteDoc = db.prepare(`DELETE FROM documents WHERE doc_id = ?`);

    for (const row of existing) {
      deleteFTS.run(row.rowid, row.text);
      deleteChunk.run(row.doc_id);
      deleteDoc.run(row.doc_id);
    }

    const insertDoc = db.prepare(`
      INSERT INTO documents(doc_id, content_hash, title, updated_at)
      VALUES(?, ?, ?, ?)
    `);
    const insertChunk = db.prepare(`
      INSERT INTO chunks(chunk_id, doc_id, text, span_start, span_end, content_hash)
      VALUES(?, ?, ?, 0, length(?), ?)
    `);
    const insertFTS = db.prepare(`INSERT INTO chunks_fts(rowid, text) VALUES(?, ?)`);
    const upsertBlob = db.prepare(`INSERT OR REPLACE INTO blobs(content_hash, text) VALUES(?, ?)`);

    const EPOCH = "1970-01-01T00:00:00.000Z";

    for (const t of SYSTEM_TOOLS) {
      const docId = `sys:tool:${t.def.name}`;
      const schema = zodToJsonSchema(t.schema, t.def.name);

      const manifestObj = {
        tool: t.def.name,
        version: t.def.version,
        type: "System Capability",
        description: t.desc,
        schema
      };

      const manifestText = canonicalize(manifestObj);
      const hash = createHash("sha256").update(manifestText).digest("hex");

      // Store blob so checkout can restore
      upsertBlob.run(hash, manifestText);

      insertDoc.run(docId, hash, `Tool: ${t.def.name}`, EPOCH);
      const info = insertChunk.run(`${docId}#def`, docId, manifestText, manifestText, hash);
      insertFTS.run((info as any).lastInsertRowid, manifestText);
    }

    // Idempotent auto-commit
    const { treeHash: currentTree, entriesJson } = git.createTreeFromCurrentState(db);
    const currentHead = git.getRef("HEAD", db);

    if (currentHead) {
      const headTree = git.getTreeHashForCommit(currentHead, db);
      if (headTree === currentTree) return;
    }

    git.saveTree(currentTree, entriesJson, db);
    const parents = currentHead ? [currentHead] : [];
    const commitHash = git.createCommit(currentTree, parents, "System Bootstrap: Tool Registry Sync", db);
    git.updateRef("main", commitHash, db);
    git.updateRef("HEAD", commitHash, db);
  });

  tx();
}
