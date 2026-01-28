import { z } from "zod";
import { git } from "../core/store/git.js";
import { store } from "../core/store/db.js";
import { buildEnvelope } from "../core/audit/envelope.js";

export const CommitIndexInputSchema = z.object({
  request_id: z.string().optional(),
  message: z.string().default("Snapshot"),
  branch: z.string().default("main"),
  expected_parent: z.string().optional()
});

export const CommitIndexTool = {
  name: "commit_index",
  version: "1.0.1",

  execute: async (input: z.infer<typeof CommitIndexInputSchema>) => {
    const db = store.db;

    const tx = db.transaction(() => {
      const currentTip = git.getRef(input.branch, db);

      if (input.expected_parent !== undefined) {
        const expected = input.expected_parent;
        const actual = currentTip ?? "";
        if (expected !== actual) {
          return {
            ok: false as const,
            error: {
              code: "ERR_REF_MISMATCH",
              message: `Branch '${input.branch}' is at ${currentTip ?? "null"}, expected ${expected}`,
              data: { current_tip: currentTip, expected }
            }
          };
        }
      }

      const parents = currentTip ? [currentTip] : [];
      const { treeHash, entriesJson, rowCount } = git.createTreeFromCurrentState(db);

      git.saveTree(treeHash, entriesJson, db);
      const commitHash = git.createCommit(treeHash, parents, input.message, db);

      git.updateRef(input.branch, commitHash, db);
      git.updateRef("HEAD", commitHash, db);

      return { ok: true as const, commitHash, treeHash, parents, rowCount };
    });

    const out = tx();

    if (!out.ok) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "commit_index",
        tool_version: "1.0.1",
        input,
        result: null,
        errors: [{ code: out.error.code, message: out.error.message, data: out.error.data }]
      });
    }

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "commit_index",
      tool_version: "1.0.1",
      input,
      result: {
        status: "committed",
        commit_hash: out.commitHash,
        tree_hash: out.treeHash,
        parents: out.parents,
        branch: input.branch,
        docs_count: out.rowCount,
        head_updated: true
      },
      provenance: [
        { source_type: "index", source_id: `refs/${input.branch}`, index_version: out.commitHash },
        { source_type: "index", source_id: "refs/HEAD", index_version: out.commitHash },
        { source_type: "index", source_id: `trees/${out.treeHash}`, content_hash: out.treeHash }
      ]
    });
  }
};
