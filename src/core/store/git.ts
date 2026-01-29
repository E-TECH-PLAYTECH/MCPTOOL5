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
   * Requires blobs(content_hash -> text) for every chunk_content_hash.
   */
  public materializeTree(treeHash: string, db?: Db) {
    const useDb = this.getDb(db);

    const entries = this.getTreeEntries(treeHash, useDb);
    if (entries === null) {
      const err = new Error(`Tree not found: ${treeHash}`);
      (err as any).code = "ERR_TREE_NOT_FOUND";
      throw err;
    }

    // Validate blobs exist for all chunk hashes
    const getBlob = useDb.prepare("SELECT text FROM blobs WHERE content_hash = ?");
    for (const e of entries) {
      const blob = getBlob.get(e.chunk_content_hash) as any;
      if (!blob) {
        const err = new Error(`Missing blob for chunk content_hash ${e.chunk_content_hash}`);
        (err as any).code = "ERR_BLOB_MISSING";
        (err as any).path = "blobs";
        throw err;
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

    const seenDocs = new Set<string>();
    const epoch = "1970-01-01T00:00:00.000Z";

    for (const e of entries) {
      if (!seenDocs.has(e.doc_id)) {
        insertDoc.run(e.doc_id, e.doc_content_hash, e.title, epoch);
        seenDocs.add(e.doc_id);
      }
      const blob = getBlob.get(e.chunk_content_hash) as any;
      insertChunk.run(
        e.chunk_id,
        e.doc_id,
        String(blob.text),
        e.span_start,
        e.span_end,
        e.chunk_content_hash
      );
    }

    // Rebuild FTS deterministically
    useDb.prepare(`INSERT INTO chunks_fts(chunks_fts) VALUES('rebuild')`).run();
  }
}

export const git = new GitOps();
