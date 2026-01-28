import { z } from "zod";
import { git } from "../core/store/git.js";
import { buildEnvelope } from "../core/audit/envelope.js";

const KNOWN_REFS = new Set(["HEAD", "main"]);

export const LogIndexInputSchema = z.object({
  request_id: z.string().optional(),
  ref: z.string().default("HEAD"),
  limit: z.number().int().min(1).max(200).default(50)
});

export const LogIndexTool = {
  name: "log_index",
  version: "1.0.3",

  execute: async (input: z.infer<typeof LogIndexInputSchema>) => {
    const resolution = git.resolveTarget(input.ref);
    if (!resolution) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "log_index",
        tool_version: "1.0.3",
        input,
        result: { ref: input.ref, resolved_commit: null, history: [], count: 0 },
        errors: [{ code: "ERR_REF_NOT_FOUND", message: `Unable to resolve '${input.ref}' to a ref or commit hash.` }]
      });
    }

    const resolved_commit = resolution.commit;

    const history: any[] = [];
    let current = resolved_commit;

    for (let i = 0; i < input.limit; i++) {
      const commit = git.getCommit(current);
      if (!commit) break;

      let parents: string[];
      try {
        parents = JSON.parse(commit.parents_json);
        if (!Array.isArray(parents) || parents.some(p => typeof p !== "string")) {
          throw new Error("parents_json not a string array");
        }
      } catch {
        return buildEnvelope({
          request_id: input.request_id,
          tool_name: "log_index",
          tool_version: "1.0.3",
          input,
          result: { ref: input.ref, resolved_commit, history: [], count: 0 },
          errors: [{
            code: "ERR_DATA_CORRUPTION",
            message: `Malformed parents_json in commit ${current}`,
            path: "parents_json"
          }]
        });
      }

      history.push({
        commit_hash: commit.commit_hash,
        tree_hash: commit.tree_hash,
        parents,
        message: commit.message,
        created_at: commit.created_at
      });

      current = parents.length ? parents[0] : "";
      if (!current) break;
    }

    const source_id =
      KNOWN_REFS.has(input.ref) ? `refs/${input.ref}` :
      resolution.kind === "ref" ? `refs/${input.ref}` :
      `commits/${resolved_commit}`;

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "log_index",
      tool_version: "1.0.3",
      input,
      result: { ref: input.ref, resolved_commit, history, count: history.length },
      provenance: [
        { source_type: "index", source_id, index_version: resolved_commit },
        { source_type: "index", source_id: `commits/${resolved_commit}`, index_version: resolved_commit }
      ]
    });
  }
};
