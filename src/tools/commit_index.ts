import { z } from "zod";
import { createHash } from "crypto";
import { store } from "../core/store/db.js";
import { git } from "../core/store/git.js";
import { buildEnvelope } from "../core/audit/envelope.js";

export const CommitIndexInputSchema = z.object({
  request_id: z.string().optional(),
  message: z.string().default("Snapshot"),
  branch: z.string().default("main"),
  expected_parent: z.string().optional()
});

export const CommitIndexTool = {
  name: "commit_index",
  version: "1.1.0",

  execute: async (input: z.infer<typeof CommitIndexInputSchema>) => {
    const db = store.db;

    const tx = db.transaction(() => {
      const currentTip = git.getRef(input.branch, db);

      if (input.expected_parent && currentTip !== input.expected_parent) {
        return {
          ok: false as const,
          envelope: buildEnvelope({
            request_id: input.request_id,
            tool_name: "commit_index",
            tool_version: "1.1.0",
            input,
            result: null,
            errors: [{
              code: "ERR_REF_MISMATCH",
              message: `Branch '${input.branch}' is at ${currentTip ?? "null"}, expected ${input.expected_parent}`,
              data: { current_tip: currentTip, expected: input.expected_parent }
            }]
          })
        };
      }

      const parents = currentTip ? [currentTip] : [];
      const { treeHash, entriesJson, rowCount } = git.createTreeFromCurrentState(db);

      git.saveTree(treeHash, entriesJson, db);

      const docRows = db.prepare(
        `SELECT doc_id, content_hash
         FROM documents
         ORDER BY doc_id ASC`
      ).all() as Array<any>;

      const chunkRows = db.prepare(
        `SELECT chunk_id, doc_id, text, span_start, span_end, content_hash
         FROM chunks
         ORDER BY doc_id ASC, chunk_id ASC`
      ).all() as Array<any>;

      const chunksByDoc = new Map<string, Array<any>>();
      for (const chunk of chunkRows) {
        const docId = String(chunk.doc_id);
        if (!chunksByDoc.has(docId)) chunksByDoc.set(docId, []);
        chunksByDoc.get(docId)!.push(chunk);
      }

      const upsertBlob = db.prepare(`INSERT OR IGNORE INTO blobs(blob_hash, bytes) VALUES(?, ?)`);
      const upsertTreeDoc = db.prepare(
        `INSERT OR IGNORE INTO tree_docs(tree_hash, doc_id, blob_hash, content_hash)
         VALUES(?, ?, ?, ?)`
      );
      const upsertTreeChunk = db.prepare(
        `INSERT OR IGNORE INTO tree_chunks(tree_hash, chunk_id, doc_id, span_start, span_end, content_hash, chunker_id)
         VALUES(?, ?, ?, ?, ?, ?, ?)`
      );

      const chunkerId = "legacy";

      for (const doc of docRows) {
        const docId = String(doc.doc_id);
        const chunks = chunksByDoc.get(docId) ?? [];
        const docText = git.buildDocumentText(chunks.map((c) => ({
          span_start: c.span_start,
          span_end: c.span_end,
          text: String(c.text)
        })));
        const docBytes = Buffer.from(docText, "utf-8");
        const blobHash = createHash("sha256").update(docBytes).digest("hex");

        upsertBlob.run(blobHash, docBytes);
        upsertTreeDoc.run(treeHash, docId, blobHash, String(doc.content_hash));
      }

      for (const chunk of chunkRows) {
        const spanStart = chunk.span_start ?? 0;
        const spanEnd = chunk.span_end ?? String(chunk.text).length;
        upsertTreeChunk.run(
          treeHash,
          String(chunk.chunk_id),
          String(chunk.doc_id),
          spanStart,
          spanEnd,
          String(chunk.content_hash),
          chunkerId
        );
      }

      const commitHash = git.createCommit(treeHash, parents, input.message, db);

      git.updateRef(input.branch, commitHash, db);
      git.updateRef("HEAD", commitHash, db);

      return {
        ok: true as const,
        commitHash,
        treeHash,
        parents,
        rowCount
      };
    });

    const out = tx();
    if (!out.ok) return (out as any).envelope;

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "commit_index",
      tool_version: "1.1.0",
      input,
      result: {
        status: "committed",
        commit_hash: out.commitHash,
        tree_hash: out.treeHash,
        parents: out.parents,
        branch: input.branch,
        entries_count: out.rowCount,
        head_updated: true
      },
      provenance: [{ source_type: "index", source_id: "refs/HEAD", index_version: out.commitHash }]
    });
  }
};
