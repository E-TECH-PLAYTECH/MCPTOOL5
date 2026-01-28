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

/**
 * Global Response Envelope.
 * outputs_hash is computed over `result` ONLY to remain stable regardless of request_id/timestamps.
 */
export function buildEnvelope(opts: EnvelopeOptions) {
  const serverVersion = opts.server_version ?? "1.0.0";
  const requestId = opts.request_id ?? uuidv4();

  const inputs_hash = computeHash(opts.input);
  const outputs_hash = computeHash(opts.result);

  return {
    request_id: requestId,
    tool_name: opts.tool_name,
    tool_version: opts.tool_version,
    server_version: serverVersion,
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
}
