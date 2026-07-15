/** Expand scalar or positional batch responses without accepting missing slots. */
export function expandManyResponse(value: unknown, count: number): unknown[] {
  if (Array.isArray(value)) {
    if (value.length !== count) {
      throw new TypeError(`batch response length ${value.length} did not match expected ${count}`);
    }
    const result = new Array<unknown>(count);
    for (let index = 0; index < count; index += 1) {
      if (!Object.hasOwn(value, index)) {
        throw new TypeError(`batch response item ${index} is missing`);
      }
      result[index] = value[index];
    }
    return result;
  }
  return Array.from({ length: count }, () => value);
}

export function arrayResponse(value: unknown): unknown[] {
  if (!Array.isArray(value)) {
    throw new TypeError("server returned an invalid array response");
  }
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.hasOwn(value, index)) {
      throw new TypeError(`server response item ${index} is missing`);
    }
  }
  return value.slice();
}
