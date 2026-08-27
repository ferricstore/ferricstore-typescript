import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import { detachDecodedBinary } from "../src/protocol-binary-detacher.js";

describe("decoded binary detachment", () => {
  it("copies small slices out of disproportionately large response storage", () => {
    const backing = Buffer.allocUnsafeSlow(1_024);
    backing.fill(0x61);
    const slice = backing.subarray(100, 132);

    const detached = detachDecodedBinary(slice, backing.byteLength) as Buffer;

    expect(detached).toEqual(slice);
    expect(detached).not.toBe(slice);
    expect(detached.buffer).not.toBe(backing.buffer);
    expect(detached.buffer.byteLength).toBe(slice.byteLength);
  });

  it("keeps large values zero-copy when they represent most of the response", () => {
    const backing = Buffer.allocUnsafeSlow(1_024);
    const slice = backing.subarray(100, 700);

    expect(detachDecodedBinary(slice, backing.byteLength)).toBe(slice);
  });

  it("detaches nested decoded containers without changing their shape", () => {
    const backing = Buffer.allocUnsafeSlow(1_024);
    backing.fill(0x62);
    const first = backing.subarray(0, 16);
    const second = backing.subarray(16, 32);
    const value = { fields: [first, { second }] };

    expect(detachDecodedBinary(value, backing.byteLength)).toEqual({
      fields: [Buffer.alloc(16, 0x62), { second: Buffer.alloc(16, 0x62) }]
    });
    expect((value.fields[0] as Buffer).buffer).not.toBe(backing.buffer);
    expect((value.fields[1] as { second: Buffer }).second.buffer).not.toBe(backing.buffer);
  });
});
