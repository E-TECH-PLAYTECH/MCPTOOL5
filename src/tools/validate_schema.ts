import { z } from "zod";
import { buildEnvelope } from "../core/audit/envelope.js";
import { createHash } from "crypto";
import { readFileSync } from "fs";
import { join } from "path";
import Ajv from "ajv";

const ajv = new Ajv({ allErrors: true, strict: false });

function loadSchema(schema_id: string) {
  const p = join(process.cwd(), "schemas", `${schema_id}.json`);
  const raw = readFileSync(p, "utf8");
  const hash = createHash("sha256").update(raw).digest("hex");
  const json = JSON.parse(raw);
  return { path: p, raw, hash, json };
}

export const ValidateSchemaInputSchema = z.object({
  request_id: z.string().optional(),
  schema_id: z.string().min(1),
  payload: z.unknown()
});

export const ValidateSchemaTool = {
  name: "validate_schema",
  version: "1.0.0",

  execute: async (input: z.infer<typeof ValidateSchemaInputSchema>) => {
    let loaded: { path: string; raw: string; hash: string; json: any };

    try {
      loaded = loadSchema(input.schema_id);
    } catch {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "validate_schema",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_NOT_FOUND", message: `Schema '${input.schema_id}' not found.` }]
      });
    }

    let validate;
    try {
      validate = ajv.compile(loaded.json);
    } catch (e: any) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "validate_schema",
        tool_version: "1.0.0",
        input,
        result: null,
        errors: [{ code: "ERR_SCHEMA_INVALID", message: e?.message ?? "Invalid schema JSON." }]
      });
    }

    const ok = !!validate(input.payload);
    const errors = (validate.errors ?? []).map(err => ({
      code: "SCHEMA_VALIDATION_FAILED",
      path: (err.instancePath || "").replace(/^\//, ""),
      message: err.message ?? "validation error"
    }));

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "validate_schema",
      tool_version: "1.0.0",
      input,
      result: {
        ok,
        errors,
        schema_id: input.schema_id,
        schema_hash: loaded.hash
      },
      provenance: [{
        source_type: "file",
        source_id: `schemas/${input.schema_id}.json`,
        content_hash: loaded.hash
      }]
    });
  }
};
