import { Buffer } from "node:buffer";
import { OPCODES, type ProtocolCommand } from "./protocol-constants.js";

/** Add the absolute planner deadline only when a native query is ready to enter the write queue. */
export function withFlowQueryDeadline(
  command: ProtocolCommand,
  timeoutMs: number | undefined,
  nowMs?: number
): ProtocolCommand {
  if (timeoutMs == null) return command;
  if (command.opcode === OPCODES.pipeline) {
    return withPipelineQueryDeadlines(command, timeoutMs, nowMs);
  }
  if (command.opcode !== OPCODES.flowQuery) return command;
  const payload = command.payload;
  if (payload instanceof Map) {
    const nextPayload = new Map(payload);
    nextPayload.set("deadline_ms", flowQueryDeadlineMs(timeoutMs, nowMs));
    return { ...command, payload: nextPayload };
  }
  if (
    typeof payload !== "object" ||
    payload == null ||
    Array.isArray(payload) ||
    Buffer.isBuffer(payload) ||
    payload instanceof Uint8Array
  ) {
    return command;
  }
  return {
    ...command,
    payload: {
      ...(payload as Record<string, unknown>),
      deadline_ms: flowQueryDeadlineMs(timeoutMs, nowMs)
    }
  };
}

function withPipelineQueryDeadlines(
  command: ProtocolCommand,
  timeoutMs: number,
  nowMs: number | undefined
): ProtocolCommand {
  const payload = plainRecord(command.payload);
  if (payload == null) return command;
  const rawCommands: unknown = payload.commands;
  if (!Array.isArray(rawCommands)) return command;
  let commands: unknown[] | undefined;
  let deadlineMs: number | undefined;
  for (let index = 0; index < rawCommands.length; index += 1) {
    const rawItem: unknown = rawCommands[index];
    const item = plainRecord(rawItem);
    if (item?.opcode !== OPCODES.flowQuery) continue;
    const body = plainRecord(item.body);
    if (body == null) continue;
    commands ??= rawCommands.slice();
    deadlineMs ??= flowQueryDeadlineMs(timeoutMs, nowMs);
    commands[index] = { ...item, body: { ...body, deadline_ms: deadlineMs } };
  }
  return commands == null
    ? command
    : { ...command, payload: { ...payload, commands } };
}

function flowQueryDeadlineMs(timeoutMs: number, nowMs?: number): number {
  return Math.min(
    Number.MAX_SAFE_INTEGER,
    (nowMs ?? Date.now()) + Math.max(1, Math.ceil(timeoutMs))
  );
}

function plainRecord(value: unknown): Record<string, unknown> | undefined {
  if (
    typeof value !== "object" ||
    value == null ||
    Array.isArray(value) ||
    Buffer.isBuffer(value) ||
    value instanceof Uint8Array
  ) {
    return undefined;
  }
  return value as Record<string, unknown>;
}
