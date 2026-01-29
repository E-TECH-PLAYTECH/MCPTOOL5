import { z } from "zod";
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

      // Ensure blobs exist for every current chunk (required for checkout)
      const blobUpsert = db.prepare(`INSERT OR REPLACE INTO blobs(content_hash, text) VALUES(?, ?)`);
      const chunkRows = db.prepare(`SELECT content_hash, text FROM chunks ORDER BY chunk_id ASC`).all() as Array<any>;
      for (const r of chunkRows) blobUpsert.run(String(r.content_hash), String(r.text));

      const parents = currentTip ? [currentTip] : [];
      const { treeHash, entriesJson, rowCount } = git.createTreeFromCurrentState(db);

      git.saveTree(treeHash, entriesJson, db);
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
