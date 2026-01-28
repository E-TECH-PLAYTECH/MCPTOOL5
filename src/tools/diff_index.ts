import { z } from "zod";
import { git } from "../core/store/git.js";
import { buildEnvelope } from "../core/audit/envelope.js";

export const DiffIndexInputSchema = z.object({
  request_id: z.string().optional(),
  from_ref: z.string(),
  to_ref: z.string()
});

function ensureSorted(entries: Array<{ doc_id: string; content_hash: string }>) {
  for (let i = 1; i < entries.length; i++) {
    if (entries[i - 1].doc_id > entries[i].doc_id) return false;
  }
  return true;
}

export const DiffIndexTool = {
  name: "diff_index",
  version: "1.0.3",

  execute: async (input: z.infer<typeof DiffIndexInputSchema>) => {
    const fromRes = git.resolveTarget(input.from_ref);
    const toRes = git.resolveTarget(input.to_ref);

    if (!fromRes || !toRes) {
      const missing: string[] = [];
      if (!fromRes) missing.push(input.from_ref);
      if (!toRes) missing.push(input.to_ref);
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "diff_index",
        tool_version: "1.0.3",
        input,
        result: null,
        errors: [{ code: "ERR_REF_NOT_FOUND", message: `Could not resolve: ${missing.join(", ")}` }]
      });
    }

    const fromCommit = git.getCommit(fromRes.commit);
    const toCommit = git.getCommit(toRes.commit);
    if (!fromCommit || !toCommit) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "diff_index",
        tool_version: "1.0.3",
        input,
        result: null,
        errors: [{ code: "ERR_COMMIT_NOT_FOUND", message: "Resolved commit hash not found in DB." }]
      });
    }

    const fromTree = fromCommit.tree_hash as string | null;
    const toTree = toCommit.tree_hash as string | null;

    if (!fromTree || !toTree) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "diff_index",
        tool_version: "1.0.3",
        input,
        result: null,
        errors: [{ code: "ERR_TREE_HASH_MISSING", message: "Commit row missing tree_hash." }]
      });
    }

    let listA: Array<{ doc_id: string; content_hash: string }> | null;
    let listB: Array<{ doc_id: string; content_hash: string }> | null;

    try {
      listA = git.getTreeEntries(fromTree);
      listB = git.getTreeEntries(toTree);
    } catch (e: any) {
      if (e?.code === "ERR_DATA_CORRUPTION") {
        return buildEnvelope({
          request_id: input.request_id,
          tool_name: "diff_index",
          tool_version: "1.0.3",
          input,
          result: null,
          errors: [{ code: "ERR_DATA_CORRUPTION", message: e.message, path: "entries_json" }]
        });
      }
      throw e;
    }

    if (listA === null || listB === null) {
      const missingTrees: string[] = [];
      if (listA === null) missingTrees.push(fromTree);
      if (listB === null) missingTrees.push(toTree);
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "diff_index",
        tool_version: "1.0.3",
        input,
        result: null,
        errors: [{ code: "ERR_TREE_NOT_FOUND", message: `Missing tree rows: ${missingTrees.join(", ")}` }]
      });
    }

    if (!ensureSorted(listA) || !ensureSorted(listB)) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "diff_index",
        tool_version: "1.0.3",
        input,
        result: null,
        errors: [{ code: "ERR_DATA_CORRUPTION", message: "Tree entries are not sorted by doc_id.", path: "entries_json" }]
      });
    }

    const added: string[] = [];
    const removed: string[] = [];
    const changed: string[] = [];

    let i = 0, j = 0;
    while (i < listA.length || j < listB.length) {
      const a = listA[i];
      const b = listB[j];

      if (!a) {
        added.push(b.doc_id);
        j++;
      } else if (!b) {
        removed.push(a.doc_id);
        i++;
      } else if (a.doc_id === b.doc_id) {
        if (a.content_hash !== b.content_hash) changed.push(a.doc_id);
        i++; j++;
      } else if (a.doc_id < b.doc_id) {
        removed.push(a.doc_id);
        i++;
      } else {
        added.push(b.doc_id);
        j++;
      }
    }

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "diff_index",
      tool_version: "1.0.3",
      input,
      result: {
        from_commit: fromRes.commit,
        to_commit: toRes.commit,
        from_tree: fromTree,
        to_tree: toTree,
        stats: { added: added.length, removed: removed.length, changed: changed.length },
        diff: { added, removed, changed }
      },
      provenance: [
        { source_type: "index", source_id: `commits/${fromRes.commit}`, index_version: fromRes.commit },
        { source_type: "index", source_id: `commits/${toRes.commit}`, index_version: toRes.commit },
        { source_type: "index", source_id: `trees/${fromTree}`, content_hash: fromTree },
        { source_type: "index", source_id: `trees/${toTree}`, content_hash: toTree }
      ]
    });
  }
};
