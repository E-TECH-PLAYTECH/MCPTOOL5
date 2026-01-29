export function toFloat32Blob(vec: number[]): Buffer {
  const f32 = new Float32Array(vec.length);
  for (let i = 0; i < vec.length; i++) f32[i] = vec[i];
  return Buffer.from(f32.buffer);
}

export function blobToFloat32Array(buf: Buffer): Float32Array {
  // Buffer may have byteOffset; slice to exact ArrayBuffer region
  const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
  return new Float32Array(ab);
}

export function dot(a: Float32Array, b: Float32Array): number {
  let s = 0;
  for (let i = 0; i < a.length; i++) s += a[i] * b[i];
  return s;
}

export function norm(a: Float32Array): number {
  return Math.sqrt(dot(a, a));
}

export function cosine(a: Float32Array, b: Float32Array): number {
  const na = norm(a);
  const nb = norm(b);
  if (na === 0 || nb === 0) return 0;
  return dot(a, b) / (na * nb);
}
