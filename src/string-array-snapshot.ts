export function snapshotOwnStringArray(
  values: readonly string[],
  name: string
): readonly string[] {
  const snapshot = new Array<string>(values.length);
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (!Object.hasOwn(values, index) || typeof value !== "string") {
      throw new TypeError(`${name} must be a dense array of own strings`);
    }
    snapshot[index] = value;
  }
  return Object.freeze(snapshot);
}
