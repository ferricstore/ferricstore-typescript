/**
 * V8 does not expose a portable maximum call-argument count. Keep legacy
 * variadic adapters below a conservative ceiling and require array dispatch
 * for larger commands instead of leaking an engine RangeError.
 */
const MAX_SAFE_VARIADIC_ARGUMENTS = 32_768;

export function assertSafeVariadicDispatch(
  argumentCount: number,
  arrayCapability: "commandArgs" | "executeCommandArgs"
): void {
  if (argumentCount <= MAX_SAFE_VARIADIC_ARGUMENTS) return;
  throw new TypeError(
    `commands with more than ${MAX_SAFE_VARIADIC_ARGUMENTS.toLocaleString("en-US")} arguments require ${arrayCapability}`
  );
}
