import { Buffer } from "node:buffer";
import { InvalidCommandError } from "./errors.js";
import { okResponse, text, textResponse, type CommandArgument } from "./internal.js";

export function bgsaveResponse(response: unknown): boolean {
  if (
    (typeof response === "string" || Buffer.isBuffer(response) || response instanceof Uint8Array) &&
    text(response) === "Background saving started"
  ) {
    return true;
  }
  return okResponse(response);
}

export function clientNameResponse(response: unknown): string | null {
  return response == null ? null : textResponse(response, "CLIENT GETNAME");
}

export function fetchOrComputeCompletionToken(
  options: { readonly computeToken: Buffer | null } | undefined
): Buffer | null {
  if (options == null || !Object.hasOwn(options, "computeToken") || options.computeToken === undefined) {
    throw new TypeError(
      "fetch-or-compute completion requires computeToken; pass null only for a legacy tokenless lease"
    );
  }
  if (options.computeToken !== null && !Buffer.isBuffer(options.computeToken)) {
    throw new TypeError("fetch-or-compute computeToken must be a Buffer or explicit null for a legacy lease");
  }
  return options.computeToken;
}

export function concatCommandArgs(
  prefix: readonly CommandArgument[],
  suffix: readonly CommandArgument[]
): CommandArgument[] {
  const args = new Array<CommandArgument>(prefix.length + suffix.length);
  for (let index = 0; index < prefix.length; index += 1) args[index] = prefix[index];
  for (let index = 0; index < suffix.length; index += 1) {
    args[prefix.length + index] = suffix[index];
  }
  return args;
}

export function unsupportedClientTracking(): never {
  throw new InvalidCommandError(
    "CLIENT TRACKING is not supported by the FerricStore native protocol; use native event subscriptions"
  );
}

export function unsupportedClientCaching(): never {
  throw new InvalidCommandError("CLIENT CACHING is not supported by the FerricStore native protocol");
}
