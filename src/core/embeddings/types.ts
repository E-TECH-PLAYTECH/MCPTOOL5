export type EmbeddingVector = number[];

export type EmbeddingRequest = {
  inputs: string[];
  model: string;
  dimensions?: number;
};

export type EmbeddingResponse = {
  model: string;
  vectors: EmbeddingVector[];
  dims: number;
};

export type EmbeddingProvider = {
  id: string;
  embed(req: EmbeddingRequest): Promise<EmbeddingResponse>;
};
