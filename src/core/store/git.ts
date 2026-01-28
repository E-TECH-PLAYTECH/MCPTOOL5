import { createHash } from "crypto";
import type Database from "better-sqlite3";
import { store } from "./db.js";
import { canonicalize } from "../audit/canonical.js";

type Db = Database.Database;

export class GitOps {
  private defaultDb: Db = store.db;

  private getDb(override?: Db): Db {
    return override ?? this.defaultDb;
  }

  public createTreeFromCurrentState(db?: Db) {
    const useDb = this.getDb(db);
    const rows = useDb
      .prepare(`SELECT doc_id, content_hash FROM documents ORDER BY doc_id ASC`)
      .all();
    const entriesJson = canonicalize(rows);
    const treeHash = createHash("sha256").update(entriesJson).digest("hex");
    return { treeHash, entriesJson, rowCount: rows.length };
  }

  public getRef(refName: string, db?: Db): string | null {
    const useDb = this.getDb(db);
    const row = useDb.prepare(`SELECT commit_hash FROM refs WHERE ref_name = ?`).get(refName) as any;
    return row ? row.commit_hash : null;
  }

  public resolveTarget(target: string, db?: Db): { kind: "ref"; commit: string } | { kind: "hash"; commit: string } | null {
    const refVal = this.getRef(target, db);
    if (refVal) return { kind: "ref", commit: refVal };
    if (/^[a-f0-9]{64}$/.test(target)) return { kind: "hash", commit: target };
    return null;
  }

  public getCommit(commitHash: string, db?: Db) {
    const useDb = this.getDb(db);
    return useDb.prepare(`SELECT * FROM commits WHERE commit_hash = ?`).get(commitHash) as any;
  }

  public getTreeHashForCommit(commitHash: string, db?: Db): string | null {
    const c = this.getCommit(commitHash, db);
    return c ? (c.tree_hash as string) : null;
  }

  /**
   * Returns:
   * - Array if found and valid
   * - null if missing
   * - throws err.code="ERR_DATA_CORRUPTION" if invalid JSON
   */
  public getTreeEntries(treeHash: string, db?: Db): Array<{ doc_id: string; content_hash: string }> | null {
    const useDb = this.getDb(db);
    const row = useDb.prepare(`SELECT entries_json FROM trees WHERE tree_hash = ?`).get(treeHash) as any;
    if (!row) return null;
    try {
      const parsed = JSON.parse(row.entries_json);
      return parsed as Array<{ doc_id: string; content_hash: string }>;
    } catch {
      const err = new Error(`Malformed entries_json in tree ${treeHash}`);
      (err as any).code = "ERR_DATA_CORRUPTION";
      throw err;
    }
  }

  public saveTree(treeHash: string, entriesJson: string, db?: Db) {
    const useDb = this.getDb(db);
    useDb
      .prepare(`INSERT OR IGNORE INTO trees(tree_hash, entries_json) VALUES(?, ?)`)
      .run(treeHash, entriesJson);
  }

  public updateRef(refName: string, commitHash: string, db?: Db) {
    const useDb = this.getDb(db);
    useDb
      .prepare(`
        INSERT INTO refs(ref_name, commit_hash) VALUES(?, ?)
        ON CONFLICT(ref_name) DO UPDATE SET commit_hash = excluded.commit_hash
      `)
      .run(refName, commitHash);
  }

  /**
   * Commit identity = SHA256(canonicalize({tree_hash, parents}))
   * message is stored but NOT hashed; created_at fixed to epoch.
   * messages are immutable: INSERT OR IGNORE.
   */
  public createCommit(treeHash: string, parents: string[], message: string, db?: Db) {
    const useDb = this.getDb(db);
    const identityPayload = canonicalize({ tree_hash: treeHash, parents });
    const commitHash = createHash("sha256").update(identityPayload).digest("hex");
    const createdAt = "1970-01-01T00:00:00.000Z";

    useDb
      .prepare(`
        INSERT OR IGNORE INTO commits(commit_hash, tree_hash, parents_json, message, created_at)
        VALUES(?, ?, ?, ?, ?)
      `)
      .run(commitHash, treeHash, canonicalize(parents), message, createdAt);

    return commitHash;
  }
}

export const git = new GitOps();
