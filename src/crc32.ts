import { Buffer } from "node:buffer";

const table = new Uint32Array(256);
for (let index = 0; index < table.length; index += 1) {
  let value = index;
  for (let bit = 0; bit < 8; bit += 1) {
    value = (value & 1) !== 0 ? 0xedb8_8320 ^ (value >>> 1) : value >>> 1;
  }
  table[index] = value >>> 0;
}

export function crc32(buffer: Buffer): number {
  let value = 0xffff_ffff;
  for (const byte of buffer) {
    value = (value >>> 8) ^ (table[(value ^ byte) & 0xff] ?? 0);
  }
  return (value ^ 0xffff_ffff) >>> 0;
}
