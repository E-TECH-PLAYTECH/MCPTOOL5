import type { EmbeddingProvider } from "./types.js";
import { OpenAIEmbeddingProvider } from "./openai.js";
import { LocalEmbeddingProvider } from "./local.js";

export class EmbeddingRegistry {
  private providers = new Map<string, EmbeddingProvider>();

  register(p: EmbeddingProvider) {
    this.providers.set(p.id, p);
  }

  get(id: string): EmbeddingProvider {
    const p = this.providers.get(id);
    if (!p) throw new Error(`Unknown embedding provider: ${id}`);
    return p;
  }

  list(): string[] {
    return [...this.providers.keys()].sort();
  }
}

export const embeddingRegistry = (() => {
  const reg = new EmbeddingRegistry();
  reg.register(new LocalEmbeddingProvider({ model: "hash-embedding" }));

  if (process.env.OPENAI_API_KEY) {
    reg.register(new OpenAIEmbeddingProvider({ model: "text-embedding-3-large" }));
    reg.register(new OpenAIEmbeddingProvider({ model: "text-embedding-3-small" }));
  }
  return reg;
})();

export const DEFAULT_EMBEDDING_PROVIDER_ID = process.env.OPENAI_API_KEY
  ? "openai:text-embedding-3-large"
  : "local:hash-embedding";
