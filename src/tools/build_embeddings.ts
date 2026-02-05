import { z } from "zod";
import { store } from "../core/store/db.js";
import { git } from "../core/store/git.js";
import { buildEnvelope } from "../core/audit/envelope.js";
import { canonicalize, computeHash } from "../core/audit/canonical.js";
import { embeddingRegistry, DEFAULT_EMBEDDING_PROVIDER_ID } from "../core/embeddings/registry.js";
import { toFloat32Blob } from "../core/embeddings/vec.js";
import { createHash } from "crypto";

const KIND = "chunk_embeddings";
const EPOCH = "1970-01-01T00:00:00.000Z";

export const BuildEmbeddingsInputSchema = z.object({
  request_id: z.string().optional(),
  ref: z.string().default("HEAD"),
  provider_id: z.string().default(DEFAULT_EMBEDDING_PROVIDER_ID),
  dimensions: z.number().int().positive().optional(),
  batch_size: z.number().int().min(1).max(2048).default(128)
});

export const BuildEmbeddingsTool = {
  name: "build_embeddings",
  version: "1.0.0",

  execute: async (input: z.infer<typeof BuildEmbeddingsInputSchema>) => {
    const resolved = git.resolveTarget(input.ref);
    if (!resolved) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "build_embeddings",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_REF_NOT_FOUND", message: `Could not resolve ref '${input.ref}'` }]
      });
    }

    const treeHash = git.getTreeHashForCommit(resolved);
    if (!treeHash) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "build_embeddings",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_TREE_HASH_MISSING", message: `Commit ${resolved} has no tree_hash` }]
      });
    }

    // Require clean tree to prevent semantic mismatch
    const { treeHash: currentTree } = git.createTreeFromCurrentState();
    if (currentTree !== treeHash) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "build_embeddings",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_WORKING_TREE_DIRTY", message: `Working tree ${currentTree} differs from target tree ${treeHash}` }]
      });
    }

    const db = store.db;

    // Deterministic chunk order
    const chunks = db.prepare(`
      SELECT chunk_id, text
      FROM chunks
      ORDER BY chunk_id ASC
    `).all() as Array<any>;

    const provider = embeddingRegistry.get(input.provider_id);

    const vectors: Array<{ chunk_id: string; vec: number[] }> = [];

    for (let i = 0; i < chunks.length; i += input.batch_size) {
      const batch = chunks.slice(i, i + input.batch_size);
      const texts = batch.map(b => String(b.text));

      const resp = await provider.embed({
        inputs: texts,
        model: provider.id.replace(/^openai:/, ""),
        dimensions: input.dimensions
      });

      for (let j = 0; j < batch.length; j++) {
        vectors.push({ chunk_id: String(batch[j].chunk_id), vec: resp.vectors[j] });
      }
    }

    const tx = db.transaction(() => {
      const treeRow = db.prepare(`SELECT tree_hash FROM trees WHERE tree_hash = ?`).get(treeHash);
      if (!treeRow) {
        return { ok: false as const, error: { code: "ERR_TREE_NOT_FOUND", message: `Tree ${treeHash} not found`, path: "trees" } };
      }

      const dims = vectors[0]?.vec.length ?? 0;
      if (dims <= 0) return { ok: false as const, error: { code: "ERR_EMBEDDING_DIMS", message: "Invalid embedding dims" } };

      const upsert = db.prepare(`
        INSERT INTO chunk_embeddings(tree_hash, chunk_id, embedding, model_id, dims, content_hash)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(tree_hash, chunk_id, model_id) DO UPDATE SET
          embedding=excluded.embedding,
          dims=excluded.dims,
          content_hash=excluded.content_hash
      `);

      for (const it of vectors) {
        const blob = toFloat32Blob(it.vec);
        const embHash = createHash("sha256").update(blob).digest("hex");
        upsert.run(treeHash, it.chunk_id, blob, provider.id, dims, embHash);
      }

      const manifest = {
        kind: KIND,
        tree_hash: treeHash,
        provider_id: provider.id,
        dims,
        dimensions: input.dimensions ?? null,
        chunk_count: vectors.length,
        tree_entries_hash: computeHash(git.getTreeEntries(treeHash) ?? [])
      };

      const manifest_json = canonicalize(manifest);
      const manifest_hash = createHash("sha256").update(manifest_json).digest("hex");

      const artifact_id = createHash("sha256")
        .update(canonicalize({ kind: KIND, tree_hash: treeHash, provider_id: provider.id, dims, manifest_hash }))
        .digest("hex");

      db.prepare(`
        INSERT OR REPLACE INTO index_artifacts(artifact_id, tree_hash, kind, model_id, manifest_json, content_hash, created_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
      `).run(artifact_id, treeHash, KIND, provider.id, manifest_json, manifest_hash, EPOCH);

      const upsertRef = db.prepare(
        `INSERT INTO artifact_refs(ref_type, ref_name, artifact_id, kind)
         VALUES(?, ?, ?, ?)
         ON CONFLICT(ref_type, ref_name, kind) DO UPDATE SET artifact_id = excluded.artifact_id`
      );

      upsertRef.run("commit", resolved, artifact_id, KIND);
      if (input.ref === "HEAD" || input.ref === "main") upsertRef.run("ref", input.ref, artifact_id, KIND);

      return { ok: true as const, artifact_id, manifest_hash, dims, chunk_count: vectors.length };
    });

    const out = tx();
    if (!out.ok) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "build_embeddings",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [out.error]
      });
    }

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "build_embeddings",
      tool_version: "1.0.0",
      input,
      result: {
        status: "built",
        ref: input.ref,
        resolved_commit: resolved,
        tree_hash: treeHash,
        artifact_id: out.artifact_id,
        provider_id: provider.id,
        dims: out.dims,
        chunk_count: out.chunk_count,
        manifest_hash: out.manifest_hash
      },
      provenance: [
        { source_type: "index", source_id: `commits/${resolved}`, index_version: resolved },
        { source_type: "index", source_id: `trees/${treeHash}`, content_hash: treeHash },
        { source_type: "db", source_id: "chunk_embeddings", artifact_id: out.artifact_id },
        { source_type: "db", source_id: "index_artifacts", artifact_id: out.artifact_id }
      ]
    });
  }
};
