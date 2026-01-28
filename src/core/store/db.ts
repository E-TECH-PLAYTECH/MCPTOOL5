import Database from "better-sqlite3";
import { join } from "path";

const DB_PATH = process.env.DB_PATH || join(process.cwd(), "data", "docs.db");

export class LocalStore {
  public readonly db: Database.Database;

  constructor() {
    this.db = new Database(DB_PATH);
    this.init();
  }

  private init() {
    this.db.pragma("journal_mode = WAL");

    // Working tree
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS documents (
        doc_id TEXT PRIMARY KEY,
        content_hash TEXT NOT NULL,
        title TEXT,
        updated_at TEXT
      );

      CREATE TABLE IF NOT EXISTS chunks (
        chunk_id TEXT PRIMARY KEY,
        doc_id TEXT NOT NULL,
        text TEXT NOT NULL,
        span_start INTEGER,
        span_end INTEGER,
        content_hash TEXT NOT NULL,
        FOREIGN KEY(doc_id) REFERENCES documents(doc_id)
      );

      CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
        text,
        content='chunks',
        content_rowid='rowid'
      );
    `);

    // Versioning DAG
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS refs (
        ref_name TEXT PRIMARY KEY,
        commit_hash TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS commits (
        commit_hash TEXT PRIMARY KEY,
        tree_hash TEXT NOT NULL,
        parents_json TEXT NOT NULL,
        message TEXT,
        created_at TEXT
      );

      CREATE TABLE IF NOT EXISTS trees (
        tree_hash TEXT PRIMARY KEY,
        entries_json TEXT NOT NULL
      );
    `);

    // Tasks (persistent; no in-memory store)
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS tasks (
        task_id TEXT PRIMARY KEY,
        idempotency_key TEXT UNIQUE,
        title TEXT NOT NULL,
        action TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL,
        next_run_at TEXT NOT NULL
      );
    `);

    // Helpful indexes
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_chunks_doc_id ON chunks(doc_id);
      CREATE INDEX IF NOT EXISTS idx_tasks_next_run_at ON tasks(next_run_at);
    `);
  }

  /** Deterministic keyword retrieval with tie-breaking. */
  public search(query: string, k: number = 8) {
    const stmt = this.db.prepare(`
      SELECT
        c.chunk_id,
        c.doc_id,
        c.text,
        c.span_start,
        c.span_end,
        c.content_hash,
        d.title,
        bm25(chunks_fts) AS score
      FROM chunks_fts
      JOIN chunks c ON chunks_fts.rowid = c.rowid
      JOIN documents d ON c.doc_id = d.doc_id
      WHERE chunks_fts MATCH @query
      ORDER BY
        bm25(chunks_fts),
        c.chunk_id ASC
      LIMIT @k
    `);

    return stmt.all({ query, k });
  }

  /** Returns HEAD commit hash or null. */
  public getHead(): string | null {
    const row = this.db.prepare(`SELECT commit_hash FROM refs WHERE ref_name='HEAD'`).get() as any;
    return row ? row.commit_hash : null;
  }
}

export const store = new LocalStore();
