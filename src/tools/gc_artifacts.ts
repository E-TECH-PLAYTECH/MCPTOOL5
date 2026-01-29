import { z } from "zod";
import { store } from "../core/store/db.js";
import { git } from "../core/store/git.js";
import { buildEnvelope } from "../core/audit/envelope.js";

export const GcArtifactsInputSchema = z.object({
  request_id: z.string().optional(),
  mode: z.enum(["dry_run", "commit"]).default("dry_run"),
  keep_refs: z.array(z.string()).optional(), // if omitted => all refs
  kinds: z.array(z.string()).optional() // if omitted => all kinds
});

export const GcArtifactsTool = {
  name: "gc_artifacts",
  version: "1.0.0",

  execute: async (input: z.infer<typeof GcArtifactsInputSchema>) => {
    const db = store.db;

    // 1) Determine root refs
    const refRows = input.keep_refs && input.keep_refs.length > 0
      ? input.keep_refs.map(r => ({ ref_name: r, commit_hash: git.getRef(r) })).filter(r => r.commit_hash !== null) as Array<any>
      : (db.prepare(`SELECT ref_name, commit_hash FROM refs ORDER BY ref_name ASC`).all() as Array<any>);

    // 2) Walk reachable commits (linear parent[0] is enough for Phase 1; still safe for DAG by visiting parents array)
    const reachableCommits = new Set<string>();
    const stack: string[] = refRows.map(r => String(r.commit_hash));

    while (stack.length > 0) {
      const h = stack.pop()!;
      if (reachableCommits.has(h)) continue;
      reachableCommits.add(h);

      const c = git.getCommit(h);
      if (!c) continue;

      let parents: string[] = [];
      try { parents = JSON.parse(c.parents_json); } catch { parents = []; }
      for (const p of parents) if (!reachableCommits.has(p)) stack.push(p);
    }

    // 3) Reachable trees
    const reachableTrees = new Set<string>();
    for (const ch of [...reachableCommits].sort()) {
      const t = git.getTreeHashForCommit(ch);
      if (t) reachableTrees.add(t);
    }

    const keepTreeList = [...reachableTrees].sort();

    // 4) Find deletions
    const kinds = input.kinds && input.kinds.length > 0 ? input.kinds : null;

    const artifacts = db.prepare(`
      SELECT artifact_id, tree_hash, kind
      FROM index_artifacts
      ${kinds ? "WHERE kind IN (" + kinds.map(() => "?").join(",") + ")" : ""}
      ORDER BY tree_hash ASC, kind ASC, artifact_id ASC
    `).all(...(kinds ?? [])) as Array<any>;

    const deleteArtifacts: Array<{ artifact_id: string; tree_hash: string; kind: string }> = [];
    for (const a of artifacts) {
      const th = String(a.tree_hash);
      if (!reachableTrees.has(th)) {
        deleteArtifacts.push({ artifact_id: String(a.artifact_id), tree_hash: th, kind: String(a.kind) });
      }
    }

    const deleteTreeHashes = [...new Set(deleteArtifacts.map(a => a.tree_hash))].sort();

    // embeddings to delete: tree_hash not reachable, optionally filter to model/kind via artifacts is not required
    const embeddings = db.prepare(`
      SELECT DISTINCT tree_hash
      FROM chunk_embeddings
      ORDER BY tree_hash ASC
    `).all() as Array<any>;

    const deleteEmbTrees = embeddings
      .map(r => String(r.tree_hash))
      .filter(th => !reachableTrees.has(th))
      .sort();

    const plan = {
      reachable: {
        refs: refRows.map(r => ({ ref_name: String(r.ref_name), commit_hash: String(r.commit_hash) })),
        commits: [...reachableCommits].sort(),
        trees: keepTreeList
      },
      delete: {
        artifact_refs: deleteArtifacts.map(a => ({ kind: a.kind, artifact_id: a.artifact_id })),
        index_artifacts: deleteArtifacts,
        chunk_embeddings_tree_hashes: deleteEmbTrees
      }
    };

    if (input.mode === "dry_run") {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "gc_artifacts",
        tool_version: "1.0.0",
        input,
        result: { mode: "dry_run", plan }
      });
    }

    const tx = db.transaction(() => {
      // Delete artifact_refs first (FK → index_artifacts)
      for (const a of deleteArtifacts) {
        db.prepare(`DELETE FROM artifact_refs WHERE artifact_id = ? AND kind = ?`).run(a.artifact_id, a.kind);
      }

      // Delete artifacts
      for (const a of deleteArtifacts) {
        db.prepare(`DELETE FROM index_artifacts WHERE artifact_id = ?`).run(a.artifact_id);
      }

      // Delete embeddings by tree hash
      for (const th of deleteEmbTrees) {
        db.prepare(`DELETE FROM chunk_embeddings WHERE tree_hash = ?`).run(th);
      }

      return {
        deleted: {
          artifact_refs: deleteArtifacts.length,
          index_artifacts: deleteArtifacts.length,
          chunk_embeddings_tree_hashes: deleteEmbTrees.length
        }
      };
    });

    const out = tx();

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "gc_artifacts",
      tool_version: "1.0.0",
      input,
      result: { mode: "commit", ...out, plan },
      provenance: [{ source_type: "db", source_id: "index_artifacts" }]
    });
  }
};
