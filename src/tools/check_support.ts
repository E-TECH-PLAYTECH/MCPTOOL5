import { z } from "zod";
import { buildEnvelope } from "../core/audit/envelope.js";

export const CheckSupportInputSchema = z.object({
  request_id: z.string().optional(),
  claims: z.array(z.string().min(1)),
  evidence: z.array(z.object({
    text: z.string(),
    provenance: z.object({
      source_id: z.string(),
      artifact_id: z.string().optional()
    })
  })),
  mode: z.enum(["conservative", "balanced"]).default("conservative")
});

function tokenize(text: string): Set<string> {
  return new Set(text.toLowerCase().split(/[\W_]+/).filter(w => w.length > 2));
}

function extractNumbers(text: string): Set<string> {
  return new Set(text.match(/\d+(\.\d+)?/g) || []);
}

export const CheckSupportTool = {
  name: "check_support",
  version: "1.0.0",

  execute: async (input: z.infer<typeof CheckSupportInputSchema>) => {
    const assessments = input.claims.map(claim => {
      const claimTokens = tokenize(claim);
      const claimNumbers = extractNumbers(claim);

      let bestScore = 0;
      let bestIdx = -1;

      for (let i = 0; i < input.evidence.length; i++) {
        const ev = input.evidence[i];
        const evTokens = tokenize(ev.text);
        const evNumbers = extractNumbers(ev.text);

        let hits = 0;
        for (const t of claimTokens) if (evTokens.has(t)) hits++;

        const score = claimTokens.size ? hits / claimTokens.size : 0;

        const missingNums = [...claimNumbers].filter(n => !evNumbers.has(n));
        const numbersOk = missingNums.length === 0;

        if (input.mode === "conservative") {
          if (numbersOk && score > bestScore) {
            bestScore = score;
            bestIdx = i;
          }
        } else {
          if (score > bestScore) {
            bestScore = score;
            bestIdx = i;
          }
        }
      }

      const threshold = input.mode === "conservative" ? 0.8 : 0.5;
      const status =
        bestScore >= threshold ? "supported" :
        bestScore > 0.2 ? "unclear" :
        "unsupported";

      const citations =
        bestIdx >= 0 && status !== "unsupported"
          ? [{
              source_id: input.evidence[bestIdx].provenance.source_id,
              artifact_id: input.evidence[bestIdx].provenance.artifact_id
            }]
          : [];

      return {
        claim,
        status,
        rationale: `overlap=${bestScore.toFixed(2)} mode=${input.mode}`,
        citations
      };
    });

    const provenance = assessments.flatMap(a => a.citations).map(c => ({
      source_type: "memory" as const,
      source_id: c.source_id,
      artifact_id: c.artifact_id
    }));

    return buildEnvelope({
      request_id: input.request_id,
      tool_name: "check_support",
      tool_version: "1.0.0",
      input,
      result: { assessments, method: "keyword_overlap_v1" },
      provenance
    });
  }
};
