import { startupLimits } from "./native-connection.js";
import {
  EMPTY_COMPACT_RESPONSE_OPCODES,
  nativeNegotiation,
  type CompactResponseOpcodes
} from "./native-negotiation.js";
import { UNAUTHENTICATED_MAX_FRAME_BYTES } from "./native-adapter-config.js";

export interface AppliedNativeCapabilities {
  readonly laneQueue?: number;
  readonly lanes?: number;
  readonly maxResponseBytes?: number;
  readonly pipelineCommands?: number;
}

/** Connection-local HELLO capabilities and pre-auth request-size enforcement. */
export class NativeConnectionCapabilities {
  compactResponseOpcodes: CompactResponseOpcodes = EMPTY_COMPACT_RESPONSE_OPCODES;
  requestFrameBytes: number;
  private negotiatedRequestFrameBytes: number;

  constructor(private readonly configuredRequestFrameBytes: number) {
    this.negotiatedRequestFrameBytes = configuredRequestFrameBytes;
    this.requestFrameBytes = Math.min(configuredRequestFrameBytes, UNAUTHENTICATED_MAX_FRAME_BYTES);
  }

  apply(value: unknown): AppliedNativeCapabilities {
    const limits = startupLimits(value);
    const negotiation = nativeNegotiation(value);
    this.compactResponseOpcodes = negotiation.compactResponseOpcodes;
    if (limits.frameBytes != null) {
      this.negotiatedRequestFrameBytes = Math.min(
        this.configuredRequestFrameBytes,
        limits.frameBytes
      );
      this.requestFrameBytes = Math.min(this.requestFrameBytes, this.negotiatedRequestFrameBytes);
    }
    return {
      ...(limits.laneQueue == null ? {} : { laneQueue: limits.laneQueue }),
      ...(limits.lanes == null ? {} : { lanes: limits.lanes }),
      ...(negotiation.maxResponseBytes == null ? {} : { maxResponseBytes: negotiation.maxResponseBytes }),
      ...(limits.pipelineCommands == null ? {} : { pipelineCommands: limits.pipelineCommands })
    };
  }

  activateAuthenticated(): void {
    this.requestFrameBytes = this.negotiatedRequestFrameBytes;
  }
}
