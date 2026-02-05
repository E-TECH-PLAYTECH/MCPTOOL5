import Database from 'better-sqlite3';
import { join } from 'path';

const DB_PATH = process.env.DB_PATH || join(process.cwd(), 'data', 'docs.db');

export class LocalStore {
  public readonly db: Database.Database;

  constructor() {
    this.db = new Database(DB_PATH);
    this.init();
  }

  private init() {
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('foreign_keys = ON');

    // --- PHASE 1 ---
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

    // --- PHASE 2 ---
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS blobs (
        blob_hash TEXT PRIMARY KEY,
        bytes BLOB NOT NULL
      );

      CREATE TABLE IF NOT EXISTS tree_docs (
        tree_hash TEXT NOT NULL,
        doc_id TEXT NOT NULL,
        blob_hash TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        PRIMARY KEY (tree_hash, doc_id),
        FOREIGN KEY(tree_hash) REFERENCES trees(tree_hash),
        FOREIGN KEY(blob_hash) REFERENCES blobs(blob_hash)
      );

      CREATE TABLE IF NOT EXISTS tree_chunks (
        tree_hash TEXT NOT NULL,
        chunk_id TEXT NOT NULL,
        doc_id TEXT NOT NULL,
        span_start INTEGER NOT NULL,
        span_end INTEGER NOT NULL,
        content_hash TEXT NOT NULL,
        chunker_id TEXT NOT NULL,
        PRIMARY KEY (tree_hash, chunk_id),
        FOREIGN KEY(tree_hash) REFERENCES trees(tree_hash)
      );
      CREATE INDEX IF NOT EXISTS idx_tree_chunks_tree_doc ON tree_chunks(tree_hash, doc_id);

      CREATE TABLE IF NOT EXISTS index_artifacts (
        artifact_id TEXT PRIMARY KEY,
        tree_hash TEXT NOT NULL,
        kind TEXT NOT NULL,
        model_id TEXT,
        manifest_json TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(tree_hash) REFERENCES trees(tree_hash)
      );
      CREATE UNIQUE INDEX IF NOT EXISTS uq_index_artifacts_tree_kind_model
        ON index_artifacts(tree_hash, kind, IFNULL(model_id, ''));

      CREATE TABLE IF NOT EXISTS artifact_refs (
        ref_type TEXT NOT NULL CHECK(ref_type IN ('ref','commit','tree')),
        ref_name TEXT NOT NULL,
        artifact_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        PRIMARY KEY(ref_type, ref_name, kind),
        FOREIGN KEY(artifact_id) REFERENCES index_artifacts(artifact_id),
        CHECK(
          (ref_type = 'ref' AND ref_name IN ('HEAD','main'))
          OR (ref_type IN ('commit','tree') AND length(ref_name) = 64)
        )
      );

      CREATE TABLE IF NOT EXISTS chunk_embeddings (
        tree_hash TEXT NOT NULL,
        chunk_id TEXT NOT NULL,
        model_id TEXT NOT NULL,
        embedding BLOB NOT NULL,
        dims INTEGER NOT NULL,
        content_hash TEXT NOT NULL,
        PRIMARY KEY (tree_hash, chunk_id, model_id),
        FOREIGN KEY(tree_hash, chunk_id) REFERENCES tree_chunks(tree_hash, chunk_id),
        FOREIGN KEY(tree_hash) REFERENCES trees(tree_hash)
      );
      CREATE INDEX IF NOT EXISTS idx_chunk_embeddings_tree_model ON chunk_embeddings(tree_hash, model_id);
    `);

    // --- PHASE 2.5: HISTORY-CORRECT FTS (PLATINUM HARDENED) ---
    this.db.exec(`
      -- A. Maintenance Gate (Singleton)
      CREATE TABLE IF NOT EXISTS fts_maintenance (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        enabled INTEGER NOT NULL DEFAULT 0 CHECK(enabled IN (0,1))
      );
      INSERT OR IGNORE INTO fts_maintenance(id, enabled) VALUES (1, 0);

      -- Protect the gate itself: Immutable Singleton
      CREATE TRIGGER IF NOT EXISTS fts_maintenance_no_delete
      BEFORE DELETE ON fts_maintenance
      BEGIN
        SELECT RAISE(ABORT, 'fts_maintenance is immutable singleton');
      END;

      CREATE TRIGGER IF NOT EXISTS fts_maintenance_no_insert
      BEFORE INSERT ON fts_maintenance
      WHEN EXISTS (SELECT 1 FROM fts_maintenance WHERE id=1)
      BEGIN
        SELECT RAISE(ABORT, 'fts_maintenance is singleton');
      END;

      -- B. Content Table
      CREATE TABLE IF NOT EXISTS fts_chunks (
        rowid        INTEGER PRIMARY KEY,
        tree_hash    TEXT NOT NULL,
        chunk_id     TEXT NOT NULL,
        doc_id       TEXT NOT NULL,
        span_start   INTEGER NOT NULL,
        span_end     INTEGER NOT NULL,
        content_hash TEXT NOT NULL,
        text         TEXT NOT NULL,
        FOREIGN KEY(tree_hash) REFERENCES trees(tree_hash),
        FOREIGN KEY(tree_hash, chunk_id) REFERENCES tree_chunks(tree_hash, chunk_id),
        UNIQUE(tree_hash, chunk_id)
      );
      CREATE INDEX IF NOT EXISTS idx_fts_chunks_tree_doc   ON fts_chunks(tree_hash, doc_id);
      CREATE INDEX IF NOT EXISTS idx_fts_chunks_tree_chunk ON fts_chunks(tree_hash, chunk_id);

      -- C. Virtual Index
      CREATE VIRTUAL TABLE IF NOT EXISTS fts_chunks_fts USING fts5(
        text,
        content='fts_chunks',
        content_rowid='rowid',
        tokenize='unicode61'
      );

      -- D. Sync Triggers (GATED: only run when enabled=1)
      CREATE TRIGGER IF NOT EXISTS fts_chunks_ai
      AFTER INSERT ON fts_chunks
      WHEN COALESCE((SELECT enabled FROM fts_maintenance WHERE id=1), 0) = 1
      BEGIN
        INSERT INTO fts_chunks_fts(rowid, text) VALUES (new.rowid, new.text);
      END;

      CREATE TRIGGER IF NOT EXISTS fts_chunks_ad
      AFTER DELETE ON fts_chunks
      WHEN COALESCE((SELECT enabled FROM fts_maintenance WHERE id=1), 0) = 1
      BEGIN
        INSERT INTO fts_chunks_fts(fts_chunks_fts, rowid, text) VALUES('delete', old.rowid, old.text);
      END;

      -- E. Content Guards (Active when Gate = 0)
      CREATE TRIGGER IF NOT EXISTS fts_chunks_gate_ins
      BEFORE INSERT ON fts_chunks
      WHEN COALESCE((SELECT enabled FROM fts_maintenance WHERE id=1), 0) = 0
      BEGIN
        SELECT RAISE(ABORT, 'fts_chunks is read-only (gate locked)');
      END;

      CREATE TRIGGER IF NOT EXISTS fts_chunks_gate_del
      BEFORE DELETE ON fts_chunks
      WHEN COALESCE((SELECT enabled FROM fts_maintenance WHERE id=1), 0) = 0
      BEGIN
        SELECT RAISE(ABORT, 'fts_chunks is read-only (gate locked)');
      END;

      CREATE TRIGGER IF NOT EXISTS fts_chunks_no_update
      BEFORE UPDATE ON fts_chunks
      BEGIN
        SELECT RAISE(ABORT, 'fts_chunks is immutable; delete and re-insert instead');
      END;

      -- F. Index Guards (Active when Gate = 0)
      CREATE TRIGGER IF NOT EXISTS fts_chunks_fts_no_ins
      BEFORE INSERT ON fts_chunks_fts
      WHEN COALESCE((SELECT enabled FROM fts_maintenance WHERE id=1), 0) = 0
      BEGIN
        SELECT RAISE(ABORT, 'fts_chunks_fts is read-only; write to fts_chunks');
      END;

      CREATE TRIGGER IF NOT EXISTS fts_chunks_fts_no_upd
      BEFORE UPDATE ON fts_chunks_fts
      WHEN COALESCE((SELECT enabled FROM fts_maintenance WHERE id=1), 0) = 0
      BEGIN
        SELECT RAISE(ABORT, 'fts_chunks_fts is read-only; write to fts_chunks');
      END;

      CREATE TRIGGER IF NOT EXISTS fts_chunks_fts_no_del
      BEFORE DELETE ON fts_chunks_fts
      WHEN COALESCE((SELECT enabled FROM fts_maintenance WHERE id=1), 0) = 0
      BEGIN
        SELECT RAISE(ABORT, 'fts_chunks_fts is read-only; write to fts_chunks');
      END;
    `);

    // TASKS & AUDIT
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS tasks (
        task_id TEXT PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE,
        task_json TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('pending','running','completed','canceled','failed')),
        next_run_at TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_tasks_status_next_run ON tasks(status, next_run_at);

      CREATE TABLE IF NOT EXISTS task_runs (
        run_id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        scheduled_for TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        status TEXT NOT NULL CHECK(status IN ('started','succeeded','failed')),
        result_json TEXT,
        result_hash TEXT,
        FOREIGN KEY(task_id) REFERENCES tasks(task_id)
      );
      CREATE INDEX IF NOT EXISTS idx_task_runs_task ON task_runs(task_id, scheduled_for);

      CREATE TABLE IF NOT EXISTS audit_log (
        request_id TEXT PRIMARY KEY,
        tool_name TEXT NOT NULL,
        tool_version TEXT NOT NULL,
        inputs_hash TEXT NOT NULL,
        outputs_hash TEXT NOT NULL,
        envelope_json TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_audit_tool ON audit_log(tool_name, created_at);
    `);
  }

  public getIndexVersion(): string | null {
    const row = this.db.prepare("SELECT commit_hash FROM refs WHERE ref_name = 'HEAD'").get();
    return row ? (row as any).commit_hash : null;
  }
}

export const store = new LocalStore();
