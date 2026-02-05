import { z } from "zod";
import { store } from "../core/store/db.js";
import { git } from "../core/store/git.js";
import { buildEnvelope } from "../core/audit/envelope.js";

export const CheckoutIndexInputSchema = z.object({
  request_id: z.string().optional(),
  target: z.string().default("HEAD"),        // ref name or commit hash
  update_branch: z.string().optional(),      // if set, moves this branch to target too
  mode: z.enum(["dry_run", "commit"]).default("commit")
});

export const CheckoutIndexTool = {
  name: "checkout_index",
  version: "1.0.0",

  execute: async (input: z.infer<typeof CheckoutIndexInputSchema>) => {
    const db = store.db;

    const commit = git.resolveTarget(input.target);
    if (!commit) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "checkout_index",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_REF_NOT_FOUND", message: `Could not resolve target '${input.target}'` }]
      });
    }

    const treeHash = git.getTreeHashForCommit(commit);
    if (!treeHash) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "checkout_index",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_TREE_HASH_MISSING", message: `Commit ${commit} has no tree_hash` }]
      });
    }

    // Preflight: ensure tree exists and blobs exist for tree docs
    let entries;
    try {
      entries = git.getTreeEntries(treeHash, db);
    } catch (e: any) {
      if (e.code === "ERR_DATA_CORRUPTION") {
        return buildEnvelope({
          request_id: input.request_id,
          tool_name: "checkout_index",
          tool_version: "1.0.0",
          input,
          result: null,
          errors: [{ code: "ERR_DATA_CORRUPTION", message: e.message, path: "trees.entries_json" }]
        });
      }
      throw e;
    }

    if (entries === null) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "checkout_index",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_TREE_NOT_FOUND", message: `Tree not found: ${treeHash}` }]
      });
    }

    const docRows = db.prepare(
      `SELECT doc_id, blob_hash
       FROM tree_docs
       WHERE tree_hash = ?
       ORDER BY doc_id ASC`
    ).all(treeHash) as Array<any>;

    const chunkRows = db.prepare(
      `SELECT chunk_id
       FROM tree_chunks
       WHERE tree_hash = ?
       LIMIT 1`
    ).get(treeHash);

    if (docRows.length === 0 || !chunkRows) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "checkout_index",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_TREE_PAYLOAD_MISSING", message: `tree_docs/tree_chunks missing for tree ${treeHash}` }]
      });
    }

    // Blob coverage check
    const getBlob = db.prepare(`SELECT 1 FROM blobs WHERE blob_hash = ?`);
    const missingBlobs: string[] = [];
    for (const doc of docRows) {
      const ok = getBlob.get(doc.blob_hash);
      if (!ok) missingBlobs.push(String(doc.blob_hash));
    }

    if (missingBlobs.length > 0) {
      missingBlobs.sort();
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "checkout_index",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{
          code: "ERR_BLOB_MISSING",
          message: `Missing blob(s) required to checkout tree ${treeHash}`,
          path: "blobs",
          data: { missing: missingBlobs.slice(0, 50), missing_count: missingBlobs.length }
        }]
      });
    }

    if (input.mode === "dry_run") {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "checkout_index",
        tool_version: "1.0.0",
        input,
        result: {
          mode: "dry_run",
          would_checkout: true,
          target_commit: commit,
          tree_hash: treeHash,
          entries_count: entries.length,
          would_update_head: true,
          would_update_branch: input.update_branch ?? null
        }
      });
    }

    const tx = db.transaction(() => {
      // Rewrite working tree
      git.materializeTree(treeHash, db);

      // Move HEAD and optionally a branch
      git.updateRef("HEAD", commit, db);
      if (input.update_branch) git.updateRef(input.update_branch, commit, db);

      return {
        checked_out: true,
        head: commit,
        branch_updated: input.update_branch ?? null
      };
    });

    const out = tx();

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "checkout_index",
      tool_version: "1.0.0",
      input,
      result: {
        mode: "commit",
        target_commit: commit,
        tree_hash: treeHash,
        entries_count: entries.length,
        ...out
      },
      provenance: [
        { source_type: "index", source_id: `commits/${commit}`, index_version: commit },
        { source_type: "index", source_id: `trees/${treeHash}`, content_hash: treeHash }
      ]
    });
  }
};
