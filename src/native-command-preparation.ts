import { requestNotSentError } from "./errors.js";
import type { CommandArgument } from "./internal.js";
import {
  buildProtocolCommand,
  encodeRequest,
  type ProtocolCommand
} from "./protocol.js";

/** Prepare a command before registering it as pending on a connection. */
export function prepareNativeCommand(
  args: readonly CommandArgument[],
  maxFrameBytes: number
): ProtocolCommand {
  try {
    return buildProtocolCommand(args, maxFrameBytes);
  } catch (error) {
    throw requestNotSentError(error);
  }
}

/** Encode a request before it enters the socket write queue. */
export function encodeNativeRequest(
  command: ProtocolCommand,
  requestId: bigint,
  maxFrameBytes: number
): Buffer {
  try {
    return encodeRequest(command, requestId, maxFrameBytes);
  } catch (error) {
    throw requestNotSentError(error);
  }
}
