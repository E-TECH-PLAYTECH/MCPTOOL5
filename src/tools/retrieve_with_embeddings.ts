import { z } from "zod";
import { store } from "../core/store/db.js";
import { git } from "../core/store/git.js";
import { buildEnvelope } from "../core/audit/envelope.js";
import { embeddingRegistry, DEFAULT_EMBEDDING_PROVIDER_ID } from "../core/embeddings/registry.js";
import { blobToFloat32Array, cosine } from "../core/embeddings/vec.js";

function minMaxNorm(vals: number[]): number[] {
  if (vals.length === 0) return [];
  let min = vals[0], max = vals[0];
  for (const v of vals) { if (v < min) min = v; if (v > max) max = v; }
  if (max === min) return vals.map(() => 0);
  return vals.map(v => (v - min) / (max - min));
}

export const RetrieveWithEmbeddingsInputSchema = z.object({
  request_id: z.string().optional(),

  query: z.string().min(1),
  k: z.number().int().min(1).max(25).default(8),

  ref: z.string().default("HEAD"),
  provider_id: z.string().default(DEFAULT_EMBEDDING_PROVIDER_ID),
  dimensions: z.number().int().positive().optional(),

  bm25_k: z.number().int().min(1).max(200).default(50),
  vector_k: z.number().int().min(1).max(500).default(50),

  // alpha=1 => pure BM25, alpha=0 => pure vector
  alpha: z.number().min(0).max(1).default(0.35)
});

export const RetrieveWithEmbeddingsTool = {
  name: "retrieve_with_embeddings",
  version: "1.0.0",

  execute: async (input: z.infer<typeof RetrieveWithEmbeddingsInputSchema>) => {
    const db = store.db;

    const commit = git.resolveTarget(input.ref);
    if (!commit) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "retrieve_with_embeddings",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_REF_NOT_FOUND", message: `Could not resolve ref '${input.ref}'` }]
      });
    }

    const treeHash = git.getTreeHashForCommit(commit);
    if (!treeHash) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "retrieve_with_embeddings",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_TREE_HASH_MISSING", message: `Commit ${commit} has no tree_hash` }]
      });
    }

    // Ensure embeddings exist for this tree/provider
    const artifact = db.prepare(`
      SELECT artifact_id, manifest_json, content_hash
      FROM index_artifacts
      WHERE tree_hash = ? AND kind = 'chunk_embeddings'
      LIMIT 1
    `).get(treeHash) as any;

    if (!artifact) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "retrieve_with_embeddings",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{
          code: "ERR_EMBEDDINGS_NOT_FOUND",
          message: `No embeddings artifact for tree ${treeHash}. Run build_embeddings first.`,
          path: "index_artifacts"
        }]
      });
    }

    const provider = embeddingRegistry.get(input.provider_id);

    // Embed the query
    const q = await provider.embed({
      inputs: [input.query],
      model: provider.id.replace(/^openai:/, ""),
      dimensions: input.dimensions
    });
    const qVec = new Float32Array(q.vectors[0]);

    // BM25 candidates (deterministic)
    const bm25Rows = store.search(input.query, input.bm25_k) as Array<any>;

    // Vector candidates: scan embeddings for this tree/provider (deterministic order)
    // Provider/dims pinned at row-level; we filter to provider + dims match.
    const embRows = db.prepare(`
      SELECT e.chunk_id, e.embedding, e.model_id, e.dims, e.content_hash,
             c.doc_id, c.text, c.span_start, c.span_end, c.content_hash AS chunk_content_hash,
             d.title
      FROM chunk_embeddings e
      JOIN chunks c ON c.chunk_id = e.chunk_id
      JOIN documents d ON d.doc_id = c.doc_id
      WHERE e.tree_hash = ? AND e.model_id = ?
      ORDER BY e.chunk_id ASC
    `).all(treeHash, provider.id) as Array<any>;

    const vecScores: Array<{ chunk_id: string; cos: number; row: any }> = [];
    for (const r of embRows) {
      const arr = blobToFloat32Array(Buffer.from(r.embedding));
      if (arr.length !== qVec.length) continue; // strict mismatch => ignore
      vecScores.push({ chunk_id: String(r.chunk_id), cos: cosine(qVec, arr), row: r });
    }

    // Take top vector_k by cosine desc, tie-break chunk_id asc
    vecScores.sort((a, b) => (b.cos - a.cos) || (a.chunk_id < b.chunk_id ? -1 : a.chunk_id > b.chunk_id ? 1 : 0));
    const vecTop = vecScores.slice(0, input.vector_k);

    // Union candidates by chunk_id
    const byId = new Map<string, any>();

    for (const r of bm25Rows) {
      byId.set(String(r.chunk_id), {
        chunk_id: String(r.chunk_id),
        doc_id: String(r.doc_id),
        title: r.title ?? null,
        text: String(r.text),
        span_start: r.span_start ?? 0,
        span_end: r.span_end ?? String(r.text).length,
        chunk_content_hash: String(r.content_hash),
        bm25: Number(r.score),
        cos: null as number | null
      });
    }

    for (const v of vecTop) {
      const r = v.row;
      const id = String(r.chunk_id);
      const prev = byId.get(id);
      if (prev) {
        prev.cos = v.cos;
      } else {
        byId.set(id, {
          chunk_id: id,
          doc_id: String(r.doc_id),
          title: r.title ?? null,
          text: String(r.text),
          span_start: r.span_start ?? 0,
          span_end: r.span_end ?? String(r.text).length,
          chunk_content_hash: String(r.chunk_content_hash),
          bm25: null as number | null,
          cos: v.cos
        });
      }
    }

    const candidates = [...byId.values()];
    const bm25Vals = candidates.map(c => (c.bm25 ?? 0));
    const cosVals = candidates.map(c => (c.cos ?? 0));

    const bm25N = minMaxNorm(bm25Vals);
    const cosN = minMaxNorm(cosVals);

    for (let i = 0; i < candidates.length; i++) {
      const b = bm25N[i];
      const v = cosN[i];
      candidates[i].hybrid = input.alpha * b + (1 - input.alpha) * v;
      candidates[i].bm25_norm = b;
      candidates[i].cos_norm = v;
    }

    // Final sort: hybrid desc, chunk_id asc
    candidates.sort((a, b) => (b.hybrid - a.hybrid) || (a.chunk_id < b.chunk_id ? -1 : a.chunk_id > b.chunk_id ? 1 : 0));
    const top = candidates.slice(0, input.k);

    const chunks = top.map(c => ({
      chunk_id: c.chunk_id,
      doc_id: c.doc_id,
      title: c.title,
      text: c.text,
      score: c.hybrid,
      signals: { bm25: c.bm25, cos: c.cos, bm25_norm: c.bm25_norm, cos_norm: c.cos_norm },
      span: { start: c.span_start, end: c.span_end },
      content_hash: c.chunk_content_hash
    }));

    const provenance = top.map(c => ({
      source_type: "index" as const,
      source_id: c.doc_id,
      artifact_id: c.chunk_id,
      score: c.hybrid,
      index_version: commit,
      content_hash: c.chunk_content_hash,
      span: { start: c.span_start, end: c.span_end }
    }));

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "retrieve_with_embeddings",
      tool_version: "1.0.0",
      input,
      result: {
        chunks,
        index_version: commit,
        tree_hash: treeHash,
        embedding_provider: provider.id,
        alpha: input.alpha,
        bm25_k: input.bm25_k,
        vector_k: input.vector_k
      },
      provenance
    });
  }
};
