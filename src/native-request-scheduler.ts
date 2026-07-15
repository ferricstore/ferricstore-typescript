import { OPCODES, type ProtocolCommand } from "./protocol-constants.js";

const MAX_REQUEST_ID = 0xffff_ffff_ffff_ffffn;

export class NativeRequestScheduler {
  private dataLane = 0;
  private requestId = 0n;

  nextRequestId(): bigint {
    this.requestId = this.requestId === MAX_REQUEST_ID ? 1n : this.requestId + 1n;
    return this.requestId;
  }

  assignLane(command: ProtocolCommand, protocolLanes: number): ProtocolCommand {
    if (command.laneId != null) return command;
    if (command.opcode < 0x0100 && command.opcode !== OPCODES.pipeline) {
      return { ...command, laneId: 0 };
    }
    this.dataLane = (this.dataLane % protocolLanes) + 1;
    return { ...command, laneId: this.dataLane };
  }
}
