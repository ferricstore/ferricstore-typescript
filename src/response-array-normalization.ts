/** Normalize a response array without silently preserving missing positions. */
export function mapDenseResponseArray(
  value: unknown[],
  transform: (item: unknown) => unknown
): unknown[] {
  const result = new Array<unknown>(value.length);
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.hasOwn(value, index)) {
      throw new TypeError(`response array item ${index} is missing`);
    }
    result[index] = transform(value[index]);
  }
  return result;
}
