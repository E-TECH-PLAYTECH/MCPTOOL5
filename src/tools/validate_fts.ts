import { z } from "zod";
import { createHash } from "crypto";
import { store } from "../core/store/db.js";
import { git } from "../core/store/git.js";
import { buildEnvelope } from "../core/audit/envelope.js";

export const ValidateFtsInputSchema = z.object({
  request_id: z.string().optional(),
  ref: z.string().default("HEAD"),
  deep_audit: z.boolean().default(false),
});

function sha256(data: string): string {
  return createHash("sha256").update(data).digest("hex");
}

// Forensic Normalization:
// Targets formatting equivalence for the specific security predicates used in guards.
function normalizeSql(sql: string): string {
  let s = sql.toLowerCase().replace(/\s+/g, "").replace(/==/g, "=");
  // Stable convergence loop for collapsing adjacent redundant parentheses
  while (true) {
    const next = s.replace(/\(\(/g, "(").replace(/\)\)/g, ")");
    if (next === s) return s;
    s = next;
  }
}

const RAW_PREDICATE = "coalesce((select enabled from fts_maintenance where id=1), 0)";
const NORM_PREDICATE = normalizeSql(RAW_PREDICATE);
const GATE_CLOSED = `${NORM_PREDICATE}=0`;
const GATE_OPEN = `${NORM_PREDICATE}=1`;

export const ValidateFtsTool = {
  name: "validate_fts",
  version: "1.9.0",

  execute: async (input: z.infer<typeof ValidateFtsInputSchema>) => {
    const db = store.db;
    const checks: any[] = [];
    const attestation: Record<string, { raw: string; norm: string }> = {};
    const bundleComponents: string[] = [];
    let status: "healthy" | "degraded" | "unhealthy" = "healthy";

    // 1. Maintenance Gate Check
    try {
      const gate = db.prepare("SELECT enabled FROM fts_maintenance WHERE id=1").get() as any;
      if (!gate) {
        checks.push({ check: "maintenance_gate", status: "fail", error: "row_missing" });
        status = "unhealthy";
      } else if (gate.enabled !== 0) {
        checks.push({
          check: "maintenance_gate",
          status: "fail",
          value: gate.enabled,
          message: "Gate is open (1). Security risk. Run 'UPDATE fts_maintenance SET enabled=0 WHERE id=1' to lock.",
        });
        status = "unhealthy";
      } else {
        checks.push({ check: "maintenance_gate", status: "pass", value: 0 });
      }
    } catch {
      checks.push({ check: "maintenance_gate", status: "fail", error: "table_missing" });
      status = "unhealthy";
    }

    // 2. Surface Area & Logic Verification
    const CRITICAL_TABLES = ["fts_maintenance", "fts_chunks", "fts_chunks_fts"];
    const ALLOWED_TRIGGERS = new Set([
      "fts_chunks_ai",
      "fts_chunks_ad",
      "fts_chunks_gate_ins",
      "fts_chunks_gate_del",
      "fts_chunks_no_update",
      "fts_chunks_fts_no_ins",
      "fts_chunks_fts_no_upd",
      "fts_chunks_fts_no_del",
      "fts_maintenance_no_delete",
      "fts_maintenance_no_insert",
    ]);

    const allTriggers = db.prepare(`
      SELECT name, tbl_name, sql
      FROM sqlite_master
      WHERE type='trigger' AND tbl_name IN (${CRITICAL_TABLES.map((t) => `'${t}'`).join(",")})
    `).all() as any[];

    const missing: string[] = [];
    const unexpected: Array<{ name: string; table: string }> = [];
    const insecure: any[] = [];
    const foundNames = new Set<string>();

    for (const t of allTriggers) {
      foundNames.add(t.name);

      // Ghost Trigger Detection: NULL SQL is un-auditable => unhealthy
      if (t.sql === null) {
        insecure.push({ name: t.name, issue: "trigger_sql_null", message: "Trigger definition is NULL/Corrupt" });
        status = "unhealthy";
        attestation[t.name] = { raw: "NULL", norm: "NULL" };
        bundleComponents.push(`trigger:${t.name}:NULL`);
        continue;
      }

      const rawSql: string = t.sql || "";
      const normSql = normalizeSql(rawSql);

      // Unexpected Trigger Check (Lockdown)
      if (!ALLOWED_TRIGGERS.has(t.name)) {
        unexpected.push({ name: t.name, table: t.tbl_name });
        attestation[t.name] = { raw: sha256(rawSql), norm: sha256(normSql) };
        bundleComponents.push(`trigger:${t.name}:${attestation[t.name].norm}`);
        continue;
      }

      // Allowed triggers: logic verification + dual attestation
      attestation[t.name] = { raw: sha256(rawSql), norm: sha256(normSql) };
      bundleComponents.push(`trigger:${t.name}:${attestation[t.name].norm}`);

      if (t.name.startsWith("fts_chunks_gate") || t.name.startsWith("fts_chunks_fts_no_")) {
        if (!normSql.includes(GATE_CLOSED)) insecure.push({ name: t.name, issue: "missing_closed_gate_check" });
      } else if (t.name === "fts_chunks_ai" || t.name === "fts_chunks_ad") {
        if (!normSql.includes(GATE_OPEN)) insecure.push({ name: t.name, issue: "missing_open_gate_check" });
      }
    }

    for (const req of ALLOWED_TRIGGERS) {
      if (!foundNames.has(req)) missing.push(req);
    }

    // 3. Precise Bundle Hash (tables by name, indexes by tbl_name; exclude sqlite_%)
    const schemaItems = db.prepare(`
      SELECT type, name, tbl_name, sql
      FROM sqlite_master
      WHERE
        (
          (type='table' AND name IN (${CRITICAL_TABLES.map((t) => `'${t}'`).join(",")}))
          OR
          (type='index' AND tbl_name IN (${CRITICAL_TABLES.map((t) => `'${t}'`).join(",")}))
        )
        AND name NOT LIKE 'sqlite_%'
    `).all() as any[];

    for (const item of schemaItems) {
      if (item.sql) {
        bundleComponents.push(`${item.type}:${item.name}:${sha256(item.sql)}`);
      }
    }

    bundleComponents.sort();
    const bundleHash = sha256(bundleComponents.join("|"));

    checks.push({ check: "triggers_exist", status: missing.length === 0 ? "pass" : "fail", missing });
    checks.push({ check: "triggers_allowlist", status: unexpected.length === 0 ? "pass" : "fail", unexpected });
    checks.push({ check: "triggers_secure", status: insecure.length === 0 ? "pass" : "fail", insecure });

    if (status !== "unhealthy") {
      if (missing.length > 0 || unexpected.length > 0 || insecure.length > 0) status = "degraded";
    }

    // 4. Canary & Deep Audit
    const commitHash = git.resolveTarget(input.ref);
    if (!commitHash) {
      return buildEnvelope({
        request_id: input.request_id,
        tool_name: "validate_fts",
        tool_version: "1.9.0",
        input,
        result: null,
        errors: [{ code: "ERR_REF_NOT_FOUND", message: `Ref ${input.ref} not found` }],
      });
    }

    const treeHash = git.getTreeHashForCommit(commitHash);

    if (treeHash) {
      const tCount = db.prepare("SELECT COUNT(*) as n FROM tree_chunks WHERE tree_hash = ?").get(treeHash) as any;
      const fCount = db.prepare("SELECT COUNT(*) as n FROM fts_chunks WHERE tree_hash = ?").get(treeHash) as any;

      checks.push({
        check: "counts",
        status: tCount.n === fCount.n ? "pass" : "fail",
        data: { tree: tCount.n, fts: fCount.n },
      });

      if (tCount.n !== fCount.n) status = "unhealthy";

      // Functional canary
      if (fCount.n > 0) {
        const chunk = db.prepare("SELECT text FROM fts_chunks WHERE tree_hash = ? LIMIT 1").get(treeHash) as any;
        if (chunk) {
          const term = chunk.text.split(/\s+/).filter((w: string) => w.length > 3)[0]?.replace(/"/g, "");
          if (term) {
            const hits = db.prepare(
              `SELECT c.rowid
               FROM fts_chunks_fts
               JOIN fts_chunks c ON c.rowid = fts_chunks_fts.rowid
               WHERE fts_chunks_fts MATCH ? AND c.tree_hash = ?
               LIMIT 1`
            ).all(`"${term}"`, treeHash);

            checks.push({ check: "canary", status: hits.length > 0 ? "pass" : "fail", term });
            if (hits.length === 0) status = "unhealthy";
          }
        }
      }

      if (input.deep_audit) {
        // A. Ghost Rows: index entry exists but content missing (global)
        const ghost = db.prepare(
          `SELECT 1
           FROM fts_chunks_fts f
           LEFT JOIN fts_chunks c ON c.rowid = f.rowid
           WHERE c.rowid IS NULL
           LIMIT 1`
        ).get();

        // B. Missing Index: content exists but index entry missing (tree-scoped)
        const missingIdx = db.prepare(
          `SELECT 1
           FROM fts_chunks c
           LEFT JOIN fts_chunks_fts f ON f.rowid = c.rowid
           WHERE f.rowid IS NULL AND c.tree_hash = ?
           LIMIT 1`
        ).get(treeHash);

        if (ghost || missingIdx) {
          checks.push({
            check: "deep_audit",
            status: "fail",
            error: "consistency_failure",
            details: { ghost_index_rows: !!ghost, missing_index_rows: !!missingIdx },
          });
          status = "unhealthy";
        } else {
          checks.push({ check: "deep_audit", status: "pass" });
        }
      }
    }

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "validate_fts",
      tool_version: "1.9.0",
      input,
      result: {
        status,
        checks,
        bundle_hash: bundleHash,
        schema_attestation: attestation,
        expected_gate_signature: { closed: GATE_CLOSED, open: GATE_OPEN },
      },
      provenance: [],
    });
  },
};
