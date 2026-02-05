import { z } from "zod";
import { store } from "../core/store/db.js";
import { git } from "../core/store/git.js";
import { buildEnvelope } from "../core/audit/envelope.js";

export const RetrieveInputSchema = z.object({
  request_id: z.string().optional(),
  query: z.string().min(1),
  k: z.number().int().min(1).max(25).default(8),
  index_version: z.string().optional()
});

export const RetrieveTool = {
  name: "retrieve",
  version: "1.3.0",

  execute: async (input: z.infer<typeof RetrieveInputSchema>) => {
    const db = store.db;
    const search = (query: string, k: number) =>
      db.prepare(`
        SELECT
          c.chunk_id, c.doc_id, c.text, c.span_start, c.span_end, c.content_hash,
          d.title,
          bm25(chunks_fts) as score
        FROM chunks_fts
        JOIN chunks c ON chunks_fts.rowid = c.rowid
        JOIN documents d ON c.doc_id = d.doc_id
        WHERE chunks_fts MATCH @query
        ORDER BY bm25(chunks_fts), c.chunk_id ASC
        LIMIT @k
      `).all({ query, k });

    const headCommit = store.getIndexVersion();
    const { treeHash: currentTree } = git.createTreeFromCurrentState();

    let effectiveVersion = headCommit || currentTree;
    const warnings: any[] = [];

    if (!headCommit) {
      warnings.push({ code: "WARN_NO_COMMITS", message: "No history found; serving working tree." });
    } else {
      const headTree = git.getTreeHashForCommit(headCommit);
      if (headTree !== currentTree) {
        warnings.push({ code: "WARN_WORKING_TREE_DIRTY", message: "Serving uncommitted working tree." });
        effectiveVersion = currentTree;
      }
    }

    if (input.index_version && input.index_version !== effectiveVersion) {
      warnings.push({ code: "WARN_VERSION_MISMATCH", message: `Requested ${input.index_version}, serving ${effectiveVersion}` });
    }

    const rows = search(input.query, input.k);

    const chunks = rows.map((row: any) => ({
      chunk_id: row.chunk_id,
      doc_id: row.doc_id,
      title: row.title ?? null,
      text: row.text,
      score: row.score,
      span: { start: row.span_start ?? 0, end: row.span_end ?? row.text.length },
      content_hash: row.content_hash
    }));

    const provenance = rows.map((row: any) => ({
      source_type: "index" as const,
      source_id: row.doc_id,
      artifact_id: row.chunk_id,
      score: row.score,
      index_version: effectiveVersion,
      content_hash: row.content_hash
    }));

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "retrieve",
      tool_version: "1.3.0",
      input,
      result: { chunks, index_version: effectiveVersion },
      provenance,
      warnings
    });
  }
};
