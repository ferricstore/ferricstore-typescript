import type { CommandArgument } from "./internal.js";

/** Copy a positional option range while rejecting holes and inherited slots. */
export function denseCommandArgumentSlice(
  args: readonly CommandArgument[],
  start: number,
  count: number,
  option: string
): CommandArgument[] {
  const values = new Array<CommandArgument>(count);
  for (let offset = 0; offset < count; offset += 1) {
    const index = start + offset;
    if (!Object.hasOwn(args, index)) throw new TypeError(`${option} values must be dense`);
    values[offset] = args[index];
  }
  return values;
}

/** Copy the remainder of a positional option while rejecting holes and inherited slots. */
export function denseCommandArgumentTail(
  args: readonly CommandArgument[],
  start: number,
  option: string
): CommandArgument[] {
  return denseCommandArgumentSlice(args, start, args.length - start, option);
}

/** Check a positional field without allowing a sparse or inherited value. */
export function hasOwnCommandArgument(
  args: readonly CommandArgument[],
  index: number,
  end: number,
  section: string
): boolean {
  if (index >= end) return false;
  if (!Object.hasOwn(args, index)) throw new TypeError(`${section} must be dense`);
  return true;
}
