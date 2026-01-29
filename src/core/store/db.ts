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
    this.db.pragma("foreign_keys = ON");

    // ==========================================
    // PHASE 0: CONTENT OBJECTS (for checkout)
    // ==========================================
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS blobs (
        content_hash TEXT PRIMARY KEY,
        text         TEXT NOT NULL
      );
    `);

    // ==========================================
    // PHASE 1: CONTENT & VERSIONING
    // ==========================================
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
        text, content='chunks', content_rowid='rowid'
      );

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

    // ==========================================
    // PHASE 2: ARTIFACTS, TASKS, AUDIT
    // ==========================================
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS index_artifacts (
        artifact_id   TEXT PRIMARY KEY,
        tree_hash     TEXT NOT NULL,
        kind          TEXT NOT NULL,
        manifest_json TEXT NOT NULL,
        content_hash  TEXT NOT NULL,
        created_at    TEXT NOT NULL,
        FOREIGN KEY(tree_hash) REFERENCES trees(tree_hash)
      );
      CREATE UNIQUE INDEX IF NOT EXISTS uq_index_artifacts_tree_kind
      ON index_artifacts(tree_hash, kind);

      CREATE TABLE IF NOT EXISTS chunk_embeddings (
        tree_hash    TEXT NOT NULL,
        chunk_id     TEXT NOT NULL,
        embedding    BLOB NOT NULL,
        model_id     TEXT NOT NULL,
        dims         INTEGER NOT NULL,
        content_hash TEXT NOT NULL,
        PRIMARY KEY (tree_hash, chunk_id),
        FOREIGN KEY(tree_hash) REFERENCES trees(tree_hash)
      );
      CREATE INDEX IF NOT EXISTS idx_chunk_embeddings_tree
      ON chunk_embeddings(tree_hash);

      CREATE TABLE IF NOT EXISTS tasks (
        task_id          TEXT PRIMARY KEY,
        idempotency_key  TEXT NOT NULL UNIQUE,
        task_json        TEXT NOT NULL,
        status           TEXT NOT NULL CHECK(status IN ('pending','running','completed','canceled','failed')),
        next_run_at      TEXT NOT NULL,
        created_at       TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_tasks_status_next_run
      ON tasks(status, next_run_at);

      CREATE TABLE IF NOT EXISTS task_runs (
        run_id        TEXT PRIMARY KEY,
        task_id       TEXT NOT NULL,
        scheduled_for TEXT NOT NULL,
        started_at    TEXT,
        finished_at   TEXT,
        status        TEXT NOT NULL CHECK(status IN ('started','succeeded','failed')),
        result_json   TEXT,
        result_hash   TEXT,
        FOREIGN KEY(task_id) REFERENCES tasks(task_id)
      );
      CREATE INDEX IF NOT EXISTS idx_task_runs_task
      ON task_runs(task_id, scheduled_for);

      CREATE TABLE IF NOT EXISTS audit_log (
        request_id    TEXT PRIMARY KEY,
        tool_name     TEXT NOT NULL,
        tool_version  TEXT NOT NULL,
        inputs_hash   TEXT NOT NULL,
        outputs_hash  TEXT NOT NULL,
        envelope_json TEXT NOT NULL,
        created_at    TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_audit_tool
      ON audit_log(tool_name, created_at);

      CREATE TABLE IF NOT EXISTS artifact_refs (
        ref_name    TEXT NOT NULL,
        artifact_id TEXT NOT NULL,
        kind        TEXT NOT NULL,
        PRIMARY KEY(ref_name, kind),
        FOREIGN KEY(artifact_id) REFERENCES index_artifacts(artifact_id)
      );
    `);
  }

  public search(query: string, k: number = 8) {
    const stmt = this.db.prepare(`
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
    `);
    return stmt.all({ query, k });
  }

  public getIndexVersion(): string | null {
    const row = this.db.prepare("SELECT commit_hash FROM refs WHERE ref_name = 'HEAD'").get();
    return row ? (row as any).commit_hash : null;
  }

  public getHead(): string | null {
    return this.getIndexVersion();
  }
}

export const store = new LocalStore();
