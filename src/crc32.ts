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

/** CRC32 a UTF-8 string range without allocating an intermediate byte array. */
export function crc32Utf8(text: string, start = 0, end = text.length): number {
  let value = 0xffff_ffff;
  for (let index = start; index < end; index += 1) {
    const first = text.charCodeAt(index);
    let point = first;
    if (first >= 0xd800 && first <= 0xdbff) {
      const second = index + 1 < end ? text.charCodeAt(index + 1) : 0;
      if (second >= 0xdc00 && second <= 0xdfff) {
        point = 0x1_0000 + ((first - 0xd800) << 10) + second - 0xdc00;
        index += 1;
      } else {
        point = 0xfffd;
      }
    } else if (first >= 0xdc00 && first <= 0xdfff) {
      point = 0xfffd;
    }

    if (point < 0x80) {
      value = updateCrc32(value, point);
    } else if (point < 0x800) {
      value = updateCrc32(value, 0xc0 | (point >>> 6));
      value = updateCrc32(value, 0x80 | (point & 0x3f));
    } else if (point < 0x1_0000) {
      value = updateCrc32(value, 0xe0 | (point >>> 12));
      value = updateCrc32(value, 0x80 | ((point >>> 6) & 0x3f));
      value = updateCrc32(value, 0x80 | (point & 0x3f));
    } else {
      value = updateCrc32(value, 0xf0 | (point >>> 18));
      value = updateCrc32(value, 0x80 | ((point >>> 12) & 0x3f));
      value = updateCrc32(value, 0x80 | ((point >>> 6) & 0x3f));
      value = updateCrc32(value, 0x80 | (point & 0x3f));
    }
  }
  return (value ^ 0xffff_ffff) >>> 0;
}

function updateCrc32(value: number, byte: number): number {
  return (value >>> 8) ^ (table[(value ^ byte) & 0xff] ?? 0);
}
