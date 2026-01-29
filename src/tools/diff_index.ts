import { z } from "zod";
import { git } from "../core/store/git.js";
import { buildEnvelope } from "../core/audit/envelope.js";

export const DiffIndexInputSchema = z.object({
  request_id: z.string().optional(),
  from_ref: z.string(),
  to_ref: z.string()
});

export const DiffIndexTool = {
  name: "diff_index",
  version: "1.0.3",

  execute: async (input: z.infer<typeof DiffIndexInputSchema>) => {
    const fromHash = git.resolveTarget(input.from_ref);
    const toHash = git.resolveTarget(input.to_ref);

    if (!fromHash || !toHash) {
      const missing: string[] = [];
      if (!fromHash) missing.push(input.from_ref);
      if (!toHash) missing.push(input.to_ref);
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "diff_index",
        tool_version: "1.0.3",
        input,
        result: null,
        errors: [{ code: "ERR_REF_NOT_FOUND", message: `Could not resolve refs: ${missing.join(", ")}` }]
      });
    }

    if (!git.getCommit(fromHash) || !git.getCommit(toHash)) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "diff_index",
        tool_version: "1.0.3",
        input,
        result: null,
        errors: [{ code: "ERR_COMMIT_NOT_FOUND", message: "Resolved commit hash not found in DB." }]
      });
    }

    const fromTree = git.getTreeHashForCommit(fromHash);
    const toTree = git.getTreeHashForCommit(toHash);
    if (!fromTree || !toTree) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "diff_index",
        tool_version: "1.0.3",
        input,
        result: null,
        errors: [{ code: "ERR_TREE_HASH_MISSING", message: "Commit exists but tree hash is null." }]
      });
    }

    let listA, listB;
    try {
      listA = git.getTreeEntries(fromTree);
      listB = git.getTreeEntries(toTree);
    } catch (e: any) {
      if (e.code === "ERR_DATA_CORRUPTION") {
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
      const missing: string[] = [];
      if (listA === null) missing.push(fromTree);
      if (listB === null) missing.push(toTree);
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "diff_index",
        tool_version: "1.0.3",
        input,
        result: null,
        errors: [{ code: "ERR_TREE_NOT_FOUND", message: `Tree data missing: ${missing.join(", ")}` }]
      });
    }

    // Diff at doc_id granularity based on doc_content_hash
    const aDocs = new Map<string, string>();
    const bDocs = new Map<string, string>();
    for (const e of listA) aDocs.set(e.doc_id, e.doc_content_hash);
    for (const e of listB) bDocs.set(e.doc_id, e.doc_content_hash);

    const all = [...new Set([...aDocs.keys(), ...bDocs.keys()])].sort();
    const added: string[] = [];
    const removed: string[] = [];
    const changed: string[] = [];

    for (const doc of all) {
      const ah = aDocs.get(doc);
      const bh = bDocs.get(doc);
      if (ah === undefined && bh !== undefined) added.push(doc);
      else if (ah !== undefined && bh === undefined) removed.push(doc);
      else if (ah !== bh) changed.push(doc);
    }

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "diff_index",
      tool_version: "1.0.3",
      input,
      result: {
        from_commit: fromHash,
        to_commit: toHash,
        stats: { added: added.length, removed: removed.length, changed: changed.length },
        diff: { added, removed, changed }
      },
      provenance: [
        { source_type: "index", source_id: `commits/${fromHash}`, index_version: fromHash },
        { source_type: "index", source_id: `commits/${toHash}`, index_version: toHash },
        { source_type: "index", source_id: `trees/${fromTree}`, content_hash: fromTree },
        { source_type: "index", source_id: `trees/${toTree}`, content_hash: toTree }
      ]
    });
  }
};
