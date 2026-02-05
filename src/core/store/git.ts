import { createHash } from "crypto";
import type Database from "better-sqlite3";
import { store } from "./db.js";
import { canonicalize } from "../audit/canonical.js";

type Db = Database.Database;

export type TreeEntry = {
  doc_id: string;
  doc_content_hash: string;
  title: string | null;
  chunk_id: string;
  chunk_content_hash: string;
  span_start: number;
  span_end: number;
};

export class GitOps {
  private defaultDb: Db = store.db;

  private getDb(override?: Db): Db {
    return override ?? this.defaultDb;
  }

  /** Snapshot current working state into deterministic tree entries */
  public createTreeFromCurrentState(db?: Db) {
    const useDb = this.getDb(db);

    const rows = useDb.prepare(`
      SELECT
        d.doc_id as doc_id,
        d.content_hash as doc_content_hash,
        d.title as title,
        c.chunk_id as chunk_id,
        c.content_hash as chunk_content_hash,
        COALESCE(c.span_start, 0) as span_start,
        COALESCE(c.span_end, length(c.text)) as span_end
      FROM documents d
      JOIN chunks c ON c.doc_id = d.doc_id
      ORDER BY d.doc_id ASC, c.chunk_id ASC
    `).all() as Array<any>;

    const entries: TreeEntry[] = rows.map(r => ({
      doc_id: String(r.doc_id),
      doc_content_hash: String(r.doc_content_hash),
      title: r.title === null || r.title === undefined ? null : String(r.title),
      chunk_id: String(r.chunk_id),
      chunk_content_hash: String(r.chunk_content_hash),
      span_start: Number(r.span_start),
      span_end: Number(r.span_end)
    }));

    const entriesJson = canonicalize(entries);
    const treeHash = createHash("sha256").update(entriesJson).digest("hex");
    return { treeHash, entriesJson, rowCount: entries.length };
  }

  public buildDocumentText(chunks: Array<{ span_start: number | null; span_end: number | null; text: string }>) {
    if (chunks.length === 0) return "";

    const normalized = chunks.map((chunk) => {
      const start = chunk.span_start ?? 0;
      const text = String(chunk.text);
      const end = chunk.span_end ?? (start + text.length);
      return { start, end, text };
    });

    let length = 0;
    for (const c of normalized) {
      length = Math.max(length, c.end, c.start + c.text.length);
    }

    const chars = Array(length).fill(" ");
    for (const c of normalized) {
      for (let i = 0; i < c.text.length; i++) {
        const idx = c.start + i;
        if (idx >= chars.length) {
          const oldLen = chars.length;
          chars.length = idx + 1;
          chars.fill(" ", oldLen);
        }
        chars[idx] = c.text[i];
      }
    }

    return chars.join("");
  }

  public getRef(refName: string, db?: Db): string | null {
    const useDb = this.getDb(db);
    const row = useDb.prepare("SELECT commit_hash FROM refs WHERE ref_name = ?").get(refName);
    return row ? (row as any).commit_hash : null;
  }

  public resolveTarget(target: string, db?: Db): string | null {
    const ref = this.getRef(target, db);
    if (ref) return ref;
    if (/^[a-f0-9]{64}$/.test(target)) return target;
    return null;
  }

  public getCommit(commitHash: string, db?: Db) {
    const useDb = this.getDb(db);
    return useDb.prepare("SELECT * FROM commits WHERE commit_hash = ?").get(commitHash) as any;
  }

  public getTreeHashForCommit(commitHash: string, db?: Db): string | null {
    const commit = this.getCommit(commitHash, db);
    return commit ? (commit.tree_hash as string) : null;
  }

  /**
   * Returns:
   * - Array: If found and parsed.
   * - null: If tree row is missing.
   * - throws ERR_DATA_CORRUPTION: If JSON invalid.
   */
  public getTreeEntries(treeHash: string, db?: Db): TreeEntry[] | null {
    const useDb = this.getDb(db);
    const row = useDb.prepare("SELECT entries_json FROM trees WHERE tree_hash = ?").get(treeHash) as any;
    if (!row) return null;

    try {
      const parsed = JSON.parse(row.entries_json) as TreeEntry[];
      if (!Array.isArray(parsed)) throw new Error("entries_json not array");
      return parsed;
    } catch {
      const err = new Error(`Malformed entries_json in tree ${treeHash}`);
      (err as any).code = "ERR_DATA_CORRUPTION";
      throw err;
    }
  }

  public saveTree(treeHash: string, entriesJson: string, db?: Db) {
    const useDb = this.getDb(db);
    useDb.prepare("INSERT OR IGNORE INTO trees(tree_hash, entries_json) VALUES(?, ?)").run(treeHash, entriesJson);
  }

  public updateRef(refName: string, commitHash: string, db?: Db) {
    const useDb = this.getDb(db);
    useDb.prepare(`
      INSERT INTO refs(ref_name, commit_hash) VALUES(?, ?)
      ON CONFLICT(ref_name) DO UPDATE SET commit_hash = excluded.commit_hash
    `).run(refName, commitHash);
  }

  /**
   * Commit identity = SHA256(canonical({tree_hash, parents}))
   * Strict determinism: created_at fixed to Epoch; messages immutable via INSERT OR IGNORE.
   */
  public createCommit(treeHash: string, parents: string[], message: string, db?: Db) {
    const useDb = this.getDb(db);
    const identityPayload = canonicalize({ tree_hash: treeHash, parents });
    const commitHash = createHash("sha256").update(identityPayload).digest("hex");
    const createdAt = "1970-01-01T00:00:00.000Z";

    useDb.prepare(`
      INSERT OR IGNORE INTO commits(commit_hash, tree_hash, parents_json, message, created_at)
      VALUES(?, ?, ?, ?, ?)
    `).run(commitHash, treeHash, canonicalize(parents), message, createdAt);

    return commitHash;
  }

  /**
   * Checkout: rewrite working tree tables to match a tree snapshot.
   * Requires blobs(blob_hash -> bytes) for every tree_docs blob_hash.
   */
  public materializeTree(treeHash: string, db?: Db) {
    const useDb = this.getDb(db);

    const entries = this.getTreeEntries(treeHash, useDb);
    if (entries === null) {
      const err = new Error(`Tree not found: ${treeHash}`);
      (err as any).code = "ERR_TREE_NOT_FOUND";
      throw err;
    }

    const docRows = useDb.prepare(
      `SELECT doc_id, blob_hash, content_hash
       FROM tree_docs
       WHERE tree_hash = ?
       ORDER BY doc_id ASC`
    ).all(treeHash) as Array<any>;

    if (docRows.length === 0) {
      const err = new Error(`Missing tree_docs for tree ${treeHash}`);
      (err as any).code = "ERR_TREE_DOCS_MISSING";
      (err as any).path = "tree_docs";
      throw err;
    }

    const chunkRows = useDb.prepare(
      `SELECT chunk_id, doc_id, span_start, span_end, content_hash
       FROM tree_chunks
       WHERE tree_hash = ?
       ORDER BY doc_id ASC, chunk_id ASC`
    ).all(treeHash) as Array<any>;

    if (chunkRows.length === 0) {
      const err = new Error(`Missing tree_chunks for tree ${treeHash}`);
      (err as any).code = "ERR_TREE_CHUNKS_MISSING";
      (err as any).path = "tree_chunks";
      throw err;
    }

    const blobByDoc = new Map<string, Buffer>();
    const getBlob = useDb.prepare("SELECT bytes FROM blobs WHERE blob_hash = ?");
    for (const doc of docRows) {
      const blob = getBlob.get(doc.blob_hash) as any;
      if (!blob) {
        const err = new Error(`Missing blob for doc ${doc.doc_id}`);
        (err as any).code = "ERR_BLOB_MISSING";
        (err as any).path = "blobs";
        throw err;
      }
      blobByDoc.set(String(doc.doc_id), Buffer.from(blob.bytes));
    }

    const titleByDoc = new Map<string, string | null>();
    for (const entry of entries) {
      if (!titleByDoc.has(entry.doc_id)) {
        titleByDoc.set(entry.doc_id, entry.title ?? null);
      }
    }

    // Clear working tree (order matters for FK)
    useDb.prepare("DELETE FROM chunks").run();
    useDb.prepare("DELETE FROM documents").run();

    // Reinsert deterministically
    const insertDoc = useDb.prepare(`
      INSERT INTO documents(doc_id, content_hash, title, updated_at) VALUES(?, ?, ?, ?)
    `);
    const insertChunk = useDb.prepare(`
      INSERT INTO chunks(chunk_id, doc_id, text, span_start, span_end, content_hash)
      VALUES(?, ?, ?, ?, ?, ?)
    `);

    const epoch = "1970-01-01T00:00:00.000Z";
    const seenDocs = new Set<string>();

    for (const doc of docRows) {
      const docId = String(doc.doc_id);
      if (!seenDocs.has(docId)) {
        insertDoc.run(docId, String(doc.content_hash), titleByDoc.get(docId) ?? null, epoch);
        seenDocs.add(docId);
      }
    }

    for (const chunk of chunkRows) {
      const docId = String(chunk.doc_id);
      const docText = blobByDoc.get(docId)!.toString("utf-8");
      const spanStart = Number(chunk.span_start);
      const spanEnd = Number(chunk.span_end);
      const chunkText = docText.substring(spanStart, spanEnd);
      insertChunk.run(
        String(chunk.chunk_id),
        docId,
        chunkText,
        spanStart,
        spanEnd,
        String(chunk.content_hash)
      );
    }

    // Rebuild FTS deterministically
    useDb.prepare(`INSERT INTO chunks_fts(chunks_fts) VALUES('rebuild')`).run();
  }
}

export const git = new GitOps();
