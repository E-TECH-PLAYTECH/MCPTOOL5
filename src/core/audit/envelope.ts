import { v4 as uuidv4 } from "uuid";
import { computeHash } from "./canonical.js";

export interface ProvenanceRecord {
  source_type: "file" | "db" | "http" | "memory" | "index";
  source_id: string;
  artifact_id?: string;
  title?: string;
  timestamp?: string;
  content_hash?: string;
  index_version?: string;
  score?: number;
  span?: { start: number; end: number };
}

export interface EnvelopeOptions {
  request_id?: string;
  tool_name: string;
  tool_version: string;
  server_version?: string;
  input: unknown;
  result: unknown;
  provenance?: ProvenanceRecord[];
  warnings?: Array<{ code: string; message: string; data?: unknown }>;
  errors?: Array<{ code: string; message: string; path?: string; data?: unknown }>;
}

const SERVER_VERSION = "1.0.0";

export function buildEnvelope(opts: EnvelopeOptions) {
  const requestId = opts.request_id ?? uuidv4();

  const inputs_hash = computeHash(opts.input);
  const outputs_hash = computeHash(opts.result);

  const envelope = {
    request_id: requestId,
    tool_name: opts.tool_name,
    tool_version: opts.tool_version,
    server_version: opts.server_version ?? SERVER_VERSION,
    inputs_hash,
    outputs_hash,
    result: opts.result,
    provenance: opts.provenance ?? [],
    warnings: opts.warnings ?? [],
    errors: opts.errors ?? [],
    metrics: {
      timestamp: new Date().toISOString()
    }
  };

  // Best-effort audit write (never throws; tool output must still return)
  queueMicrotask(async () => {
    try {
      const mod = await import("../store/db.js");
      const db = mod.store.db;
      db.prepare(`
        INSERT OR REPLACE INTO audit_log(
          request_id, tool_name, tool_version, inputs_hash, outputs_hash, envelope_json, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?)
      `).run(
        envelope.request_id,
        envelope.tool_name,
        envelope.tool_version,
        envelope.inputs_hash,
        envelope.outputs_hash,
        JSON.stringify(envelope),
        envelope.metrics.timestamp
      );
    } catch {
      // swallow
    }
  });

  return envelope;
}
