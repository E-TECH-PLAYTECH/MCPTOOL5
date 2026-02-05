import { z } from "zod";
import { createHash } from "crypto";
import { git } from "../core/store/git.js";
import { store } from "../core/store/db.js";
import { buildEnvelope } from "../core/audit/envelope.js";
import { canonicalize } from "../core/audit/canonical.js";

const EPOCH = "1970-01-01T00:00:00.000Z";
const TOKENIZER = "unicode61";
const FTS_KIND = "fts";
const ROWID_STRATEGY = "sha256:u63:collision-check:v1";
const FTS_SYNC_STRATEGY = "triggers:maintenance-gate:v1";

export const BuildFtsTreeInputSchema = z.object({
  request_id: z.string().optional(),
  ref: z.string().default("HEAD"),
  force_rebuild: z.boolean().default(false),
  created_at: z.string().datetime().optional(),
});

function sha256(data: Buffer | string): string {
  return createHash("sha256").update(data).digest("hex");
}

function rowidFor(treeHash: string, chunkId: string, attempt: number): bigint {
  const h = createHash("sha256").update(`${treeHash}:${chunkId}:${attempt}`).digest();
  const u64 = h.readBigUInt64LE(0);
  const u63 = (u64 & BigInt("0x7FFFFFFFFFFFFFFF")) || BigInt(1);
  return u63;
}

export const BuildFtsTreeTool = {
  name: "build_fts_tree",
  version: "1.9.0",

  execute: async (input: z.infer<typeof BuildFtsTreeInputSchema>) => {
    const db = store.db;

    // 1) Resolve Target
    const commitHash = git.resolveTarget(input.ref);
    if (!commitHash) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "build_fts_tree",
        tool_version: "1.9.0",
        input,
        result: null,
        errors: [{ code: "ERR_REF_NOT_FOUND", message: `Could not resolve ref: ${input.ref}` }],
      });
    }

    const treeHash = git.getTreeHashForCommit(commitHash);
    if (!treeHash) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "build_fts_tree",
        tool_version: "1.9.0",
        input,
        result: null,
        errors: [{ code: "ERR_TREE_MISSING", message: `Commit ${commitHash} has no tree_hash.` }],
      });
    }

    // 2) Ensure frozen state
    const frozen = db.prepare(`SELECT 1 FROM tree_chunks WHERE tree_hash = ? LIMIT 1`).get(treeHash);
    if (!frozen) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "build_fts_tree",
        tool_version: "1.9.0",
        input,
        result: null,
        errors: [{ code: "ERR_NOT_FROZEN", message: `Missing tree_chunks for tree ${treeHash}.` }],
      });
    }

    // 3) Idempotency & Dirty Check (Clean Slate)
    if (!input.force_rebuild) {
      // A. Artifact Check
      const existing = db.prepare(
        `SELECT artifact_id, content_hash AS payload_hash
         FROM index_artifacts
         WHERE tree_hash = ? AND kind = ? AND model_id IS NULL
         LIMIT 1`
      ).get(treeHash, FTS_KIND) as any;

      if (existing?.artifact_id) {
        const currentPayload = db.prepare(
          `SELECT chunk_id, content_hash FROM fts_chunks WHERE tree_hash = ? ORDER BY chunk_id ASC`
        ).all(treeHash);
        const recomputed = sha256(Buffer.from(canonicalize(currentPayload), "utf-8"));

        if (recomputed !== existing.payload_hash) {
          return buildEnvelope({
            request_id: input.request_id,
            tool_name: "build_fts_tree",
            tool_version: "1.9.0",
            input,
            result: null,
            errors: [{
              code: "ERR_ARTIFACT_DRIFT",
              message: "FTS artifact exists but content tables mismatch.",
              data: { expected: existing.payload_hash, got: recomputed },
            }],
          });
        }

        return buildEnvelope({
          request_id: input.request_id,
          tool_name: "build_fts_tree",
          tool_version: "1.9.0",
          input,
          result: { status: "skipped", reason: "artifact_exists", tree_hash: treeHash, artifact_id: existing.artifact_id },
          provenance: [{ source_type: "index", source_id: `trees/${treeHash}`, content_hash: treeHash }],
        });
      }

      // B. Dirty State Preflight
      const dirtyCount = db.prepare(`SELECT COUNT(*) as n FROM fts_chunks WHERE tree_hash = ?`).get(treeHash) as any;
      if (dirtyCount.n > 0) {
        return buildEnvelope({
          request_id: input.request_id,
          tool_name: "build_fts_tree",
          tool_version: "1.9.0",
          input,
          result: null,
          errors: [{
            code: "ERR_DIRTY_STATE",
            message: "Orphan FTS data found without artifact. Run with force_rebuild=true to clean.",
            data: { tree_hash: treeHash, orphan_rows: dirtyCount.n },
          }],
        });
      }
    }

    let chunksIndexed = 0;
    let artifactId = "";
    let payloadHash = "";

    try {
      db.transaction(() => {
        // A) UNLOCK GATE (Checked)
        const gateResult = db.prepare(`UPDATE fts_maintenance SET enabled = 1 WHERE id = 1`).run();
        if (gateResult.changes !== 1) throw { code: "ERR_GATE_MISSING", message: "fts_maintenance singleton missing or corrupted." };

        try {
          if (input.force_rebuild) {
            db.prepare(`DELETE FROM fts_chunks WHERE tree_hash = ?`).run(treeHash);
          }

          const rows = db.prepare(
            `SELECT
               tc.chunk_id,
               tc.doc_id,
               tc.span_start,
               tc.span_end,
               tc.content_hash AS expected_chunk_hash,
               b.bytes AS doc_bytes
             FROM tree_chunks tc
             JOIN tree_docs td ON td.tree_hash = tc.tree_hash AND td.doc_id = tc.doc_id
             JOIN blobs b ON b.blob_hash = td.blob_hash
             WHERE tc.tree_hash = ?
             ORDER BY tc.chunk_id ASC`
          ).all(treeHash) as Array<{
            chunk_id: string;
            doc_id: string;
            span_start: number;
            span_end: number;
            expected_chunk_hash: string;
            doc_bytes: Buffer;
          }>;

          const insertContent = db.prepare(
            `INSERT INTO fts_chunks (rowid, tree_hash, chunk_id, doc_id, span_start, span_end, content_hash, text)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
          );

          const checkRowId = db.prepare(`SELECT tree_hash, chunk_id FROM fts_chunks WHERE rowid = ?`);
          const checkUnique = db.prepare(`SELECT rowid, content_hash FROM fts_chunks WHERE tree_hash = ? AND chunk_id = ?`);

          const payloadRows: Array<{ chunk_id: string; content_hash: string }> = [];

          for (const r of rows) {
            const docText = r.doc_bytes.toString("utf-8").normalize("NFKC");
            const chunkText = docText.substring(r.span_start, r.span_end);

            if (sha256(Buffer.from(chunkText, "utf-8")) !== r.expected_chunk_hash) {
              throw { code: "ERR_DATA_CORRUPTION", message: `FTS hydration mismatch for chunk ${r.chunk_id}` };
            }

            let attempt = 0;
            let rowid = rowidFor(treeHash, r.chunk_id, attempt);
            let inserted = false;

            while (!inserted && attempt < 10) {
              try {
                insertContent.run(
                  rowid,
                  treeHash,
                  r.chunk_id,
                  r.doc_id,
                  r.span_start,
                  r.span_end,
                  r.expected_chunk_hash,
                  chunkText
                );
                inserted = true;
              } catch (e: any) {
                if (e.code === "SQLITE_CONSTRAINT_PRIMARYKEY") {
                  const existing = checkRowId.get(rowid) as any;
                  if (existing && existing.tree_hash === treeHash && existing.chunk_id === r.chunk_id) inserted = true;
                  else { attempt++; rowid = rowidFor(treeHash, r.chunk_id, attempt); }
                } else if (e.code === "SQLITE_CONSTRAINT_UNIQUE") {
                  const existing = checkUnique.get(treeHash, r.chunk_id) as any;
                  if (existing && existing.content_hash === r.expected_chunk_hash) inserted = true;
                  else throw { code: "ERR_DATA_CORRUPTION", message: `FTS content conflict for chunk ${r.chunk_id}` };
                } else {
                  throw e;
                }
              }
            }

            if (!inserted) throw { code: "ERR_ROWID_COLLISION", message: `Could not assign deterministic rowid for ${r.chunk_id}` };

            payloadRows.push({ chunk_id: r.chunk_id, content_hash: r.expected_chunk_hash });
            chunksIndexed++;
          }

          // B) Completeness (bidirectional safety)
          const missing = db.prepare(
            `SELECT tc.chunk_id
             FROM tree_chunks tc
             LEFT JOIN fts_chunks fc ON fc.tree_hash = tc.tree_hash AND fc.chunk_id = tc.chunk_id
             WHERE tc.tree_hash = ? AND fc.chunk_id IS NULL
             LIMIT 1`
          ).get(treeHash) as any;
          if (missing) throw { code: "ERR_FTS_INCOMPLETE", message: "FTS build incomplete.", data: { tree_hash: treeHash, missing: missing.chunk_id } };

          const extra = db.prepare(
            `SELECT fc.chunk_id
             FROM fts_chunks fc
             LEFT JOIN tree_chunks tc ON tc.tree_hash = fc.tree_hash AND tc.chunk_id = fc.chunk_id
             WHERE fc.tree_hash = ? AND tc.chunk_id IS NULL
             LIMIT 1`
          ).get(treeHash) as any;
          if (extra) throw { code: "ERR_FTS_EXTRA_ROWS", message: "FTS build corrupted: Extra chunks found.", data: { tree_hash: treeHash, extra: extra.chunk_id } };

          // C) Register artifact
          payloadHash = sha256(Buffer.from(canonicalize(payloadRows), "utf-8"));

          const manifest = {
            kind: FTS_KIND,
            tokenizer: TOKENIZER,
            tree_hash: treeHash,
            payload_hash: payloadHash,
            chunk_count: payloadRows.length,
            rowid_strategy: ROWID_STRATEGY,
            fts_sync: FTS_SYNC_STRATEGY,
          };

          const manifestJson = canonicalize(manifest);
          const idInput = canonicalize({ manifest, payload_hash: payloadHash });
          artifactId = sha256(Buffer.from(idInput, "utf-8"));
          const createdAt = input.created_at ?? EPOCH;

          db.prepare(
            `INSERT OR REPLACE INTO index_artifacts
             (artifact_id, tree_hash, kind, model_id, manifest_json, content_hash, created_at)
             VALUES (?, ?, ?, NULL, ?, ?, ?)`
          ).run(artifactId, treeHash, FTS_KIND, manifestJson, payloadHash, createdAt);

          const upsertRef = db.prepare(
            `INSERT INTO artifact_refs (ref_type, ref_name, artifact_id, kind)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(ref_type, ref_name, kind) DO UPDATE SET artifact_id = excluded.artifact_id`
          );

          upsertRef.run("commit", commitHash, artifactId, FTS_KIND);
          if (input.ref === "HEAD" || input.ref === "main") upsertRef.run("ref", input.ref, artifactId, FTS_KIND);
        } finally {
          // D) LOCK GATE (Silent Swallow)
          try { db.prepare(`UPDATE fts_maintenance SET enabled = 0 WHERE id = 1`).run(); } catch {}
        }
      })();
    } catch (e: any) {
      try { db.prepare(`UPDATE fts_maintenance SET enabled = 0 WHERE id = 1`).run(); } catch {}
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "build_fts_tree",
        tool_version: "1.9.0",
        input,
        result: null,
        errors: [{ code: e.code || "ERR_BUILD_FAILED", message: e.message, data: e.data }],
      });
    }

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "build_fts_tree",
      tool_version: "1.9.0",
      input,
      result: {
        status: "success",
        tree_hash: treeHash,
        resolved_commit: commitHash,
        artifact_id: artifactId,
        chunks_indexed: chunksIndexed,
        payload_hash: payloadHash,
      },
      provenance: [{ source_type: "db", source_id: `index_artifacts/${artifactId}`, content_hash: payloadHash }],
    });
  },
};
