import { createHash } from "crypto";
import type { EmbeddingProvider, EmbeddingRequest, EmbeddingResponse } from "./types.js";

function normalizeDims(input: number | undefined, fallback: number): number {
  if (typeof input === "number" && Number.isInteger(input) && input > 0) return input;
  return fallback;
}

function deriveVector(text: string, dims: number): number[] {
  const out = new Array<number>(dims);
  let filled = 0;
  let counter = 0;

  while (filled < dims) {
    const hash = createHash("sha256")
      .update(text)
      .update(":")
      .update(String(counter))
      .digest();

    for (let i = 0; i < hash.length && filled < dims; i += 4) {
      const value = hash.readUInt32BE(i);
      const normalized = (value / 0xffffffff) * 2 - 1;
      out[filled] = normalized;
      filled += 1;
    }

    counter += 1;
  }

  return out;
}

export class LocalEmbeddingProvider implements EmbeddingProvider {
  public readonly id: string;
  private readonly model: string;
  private readonly defaultDims: number;

  constructor(opts: { model: string; dims?: number }) {
    this.model = opts.model;
    this.id = `local:${opts.model}`;
    this.defaultDims = normalizeDims(opts.dims, 256);
  }

  async embed(req: EmbeddingRequest): Promise<EmbeddingResponse> {
    if (req.inputs.length === 0) throw new Error("embed(): empty inputs");

    const dims = normalizeDims(req.dimensions, this.defaultDims);
    const vectors = req.inputs.map((input) => deriveVector(input, dims));

    return { model: this.model, vectors, dims };
  }
}
