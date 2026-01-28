import { z } from "zod";
import { buildEnvelope } from "../core/audit/envelope.js";
import { createHash } from "crypto";
import { readFileSync } from "fs";
import { join } from "path";

const PolicySchema = z.object({
  id: z.string().min(1),
  version: z.string().min(1),
  rules: z.array(z.object({
    type: z.string().min(1),
    pattern: z.string().min(1)
  })).min(1)
});

function loadPolicy(policy_id: string) {
  const p = join(process.cwd(), "policies", `${policy_id}.json`);
  const raw = readFileSync(p, "utf8");
  const fileHash = createHash("sha256").update(raw).digest("hex");
  const json = JSON.parse(raw);
  const parsed = PolicySchema.parse(json);
  return { path: p, raw, fileHash, policy: parsed };
}

export const RedactInputSchema = z.object({
  request_id: z.string().optional(),
  text: z.string(),
  policy_id: z.string().default("default"),
  mode: z.enum(["mask", "remove", "tokenize"]).default("mask"),
  return_map: z.boolean().default(true)
});

export const RedactTool = {
  name: "redact",
  version: "1.0.0",

  execute: async (input: z.infer<typeof RedactInputSchema>) => {
    let loaded;
    try {
      loaded = loadPolicy(input.policy_id);
    } catch {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "redact",
        tool_version: "1.0.0",
        input: { ...input, text: "[OMITTED_FOR_PRIVACY]" },
        result: null,
        errors: [{ code: "ERR_NOT_FOUND", message: `Policy '${input.policy_id}' not found or invalid.` }]
      });
    }

    const rules = loaded.policy.rules.map(r => ({
      type: r.type,
      regex: new RegExp(r.pattern, "g")
    }));

    const matches: Array<{ start: number; end: number; type: string }> = [];
    for (const rule of rules) {
      for (const m of input.text.matchAll(rule.regex)) {
        if (m.index !== undefined) {
          matches.push({ start: m.index, end: m.index + m[0].length, type: rule.type });
        }
      }
    }

    matches.sort((a, b) => a.start - b.start);

    let out = "";
    let cursor = 0;
    const redaction_map: any[] = [];

    for (const m of matches) {
      if (m.start < cursor) continue;

      out += input.text.substring(cursor, m.start);

      let replacement = "";
      if (input.mode === "mask") replacement = `[REDACTED:${m.type.toUpperCase()}]`;
      else if (input.mode === "tokenize") replacement = `[TOK:${m.type}]`;

      out += replacement;

      if (input.return_map) {
        redaction_map.push({
          type: m.type,
          original_span: { start: m.start, end: m.end },
          replacement
        });
      }

      cursor = m.end;
    }

    out += input.text.substring(cursor);

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "redact",
      tool_version: "1.0.0",
      input: { ...input, text: "[OMITTED_FOR_PRIVACY]" },
      result: {
        redacted_text: out,
        redaction_map: input.return_map ? redaction_map : undefined,
        policy_id: loaded.policy.id,
        policy_version: loaded.policy.version,
        policy_hash: loaded.fileHash
      },
      provenance: [{
        source_type: "file",
        source_id: `policies/${loaded.policy.id}.json`,
        content_hash: loaded.fileHash
      }]
    });
  }
};
