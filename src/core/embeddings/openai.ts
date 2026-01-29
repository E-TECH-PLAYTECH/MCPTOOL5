import type { EmbeddingProvider, EmbeddingRequest, EmbeddingResponse } from "./types.js";

type OpenAIItem = { index: number; embedding: number[] };
type OpenAIResp = { data: OpenAIItem[]; model: string };

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

export class OpenAIEmbeddingProvider implements EmbeddingProvider {
  public readonly id: string;
  private readonly apiKey: string;
  private readonly baseUrl: string;
  private readonly model: string;

  constructor(opts: { model: string; baseUrl?: string }) {
    this.model = opts.model;
    this.id = `openai:${opts.model}`;
    this.apiKey = requireEnv("OPENAI_API_KEY");
    this.baseUrl = (opts.baseUrl ?? "https://api.openai.com").replace(/\/+$/, "");
  }

  async embed(req: EmbeddingRequest): Promise<EmbeddingResponse> {
    if (req.inputs.length === 0) throw new Error("embed(): empty inputs");

    const body: any = {
      model: this.model,
      input: req.inputs,
      encoding_format: "float"
    };
    if (typeof req.dimensions === "number") body.dimensions = req.dimensions;

    const resp = await fetch(`${this.baseUrl}/v1/embeddings`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
    });

    const text = await resp.text();
    if (!resp.ok) throw new Error(`OpenAI embeddings error (${resp.status}): ${text}`);

    const json = JSON.parse(text) as OpenAIResp;
    const data = [...json.data].sort((a, b) => a.index - b.index);
    const vectors = data.map(d => d.embedding);

    const dims = vectors[0]?.length ?? 0;
    if (dims <= 0) throw new Error("Invalid embedding dims");
    for (const v of vectors) if (v.length !== dims) throw new Error("Inconsistent embedding dims");

    if (vectors.length !== req.inputs.length) throw new Error("Embedding count mismatch");

    return { model: json.model, vectors, dims };
  }
}
