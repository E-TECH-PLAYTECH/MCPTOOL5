import { z } from "zod";
import { git } from "../core/store/git.js";
import { buildEnvelope } from "../core/audit/envelope.js";

export const LogIndexInputSchema = z.object({
  request_id: z.string().optional(),
  ref: z.string().default("HEAD"),
  limit: z.number().int().min(1).max(50).default(10)
});

export const LogIndexTool = {
  name: "log_index",
  version: "1.0.3",

  execute: async (input: z.infer<typeof LogIndexInputSchema>) => {
    const isNamedRef = git.getRef(input.ref) !== null;
    let currentHash = git.resolveTarget(input.ref);
    const resolvedRef = currentHash ?? null;

    if (!currentHash) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "log_index",
        tool_version: "1.0.3",
        input,
        result: { ref: input.ref, resolved_commit: null, history: [], count: 0 },
        errors: [{ code: "ERR_REF_NOT_FOUND", message: `Ref '${input.ref}' could not be resolved.` }]
      });
    }

    const history: any[] = [];
    let count = 0;

    while (currentHash && count < input.limit) {
      const commit = git.getCommit(currentHash);
      if (!commit) break;

      let parents: string[] = [];
      try {
        parents = JSON.parse(commit.parents_json);
      } catch {
        return buildEnvelope({
          request_id: input.request_id,
          tool_name: "log_index",
          tool_version: "1.0.3",
          input,
          result: { ref: input.ref, resolved_commit: resolvedRef, history: [], count: 0 },
          errors: [{ code: "ERR_DATA_CORRUPTION", message: `Malformed parents_json in commit ${currentHash}`, path: "parents_json" }]
        });
      }

      history.push({
        commit_hash: commit.commit_hash,
        tree_hash: commit.tree_hash,
        message: commit.message,
        parents,
        created_at: commit.created_at
      });

      currentHash = parents.length > 0 ? parents[0] : null;
      count++;
    }

    const sourceId = isNamedRef ? `refs/${input.ref}` : `commits/${resolvedRef}`;

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "log_index",
      tool_version: "1.0.3",
      input,
      result: { ref: input.ref, resolved_commit: resolvedRef, history, count: history.length },
      provenance: [{ source_type: "index", source_id: sourceId, index_version: resolvedRef! }]
    });
  }
};
