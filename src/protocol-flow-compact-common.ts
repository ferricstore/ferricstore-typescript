import * as core from "./protocol-core.js";

export function compactManyRequestTag(
  value: unknown,
  regularTag: number,
  okOnSuccessTag: number
): number | undefined {
  const mode = value == null ? "" : core.asText(value).toUpperCase();
  if (mode === "") return regularTag;
  return mode === "OK_ON_SUCCESS" ? okOnSuccessTag : undefined;
}
