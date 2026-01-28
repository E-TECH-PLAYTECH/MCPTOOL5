import stringify from "fast-json-stable-stringify";
import { createHash } from "crypto";

/** Deterministic JSON serialization (sorted keys, no whitespace). */
export function canonicalize(data: unknown): string {
  if (data === undefined) return "";
  return stringify(data as any);
}

/** SHA256 of canonical JSON. */
export function computeHash(data: unknown): string {
  const json = canonicalize(data);
  return createHash("sha256").update(json).digest("hex");
}
