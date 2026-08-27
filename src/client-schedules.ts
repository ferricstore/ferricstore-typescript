import type {
  ScheduleFireDueOptions,
  ScheduleFireDueResult,
  ScheduleFireOptions,
  ScheduleFireResult,
  ScheduleListOptions,
  ScheduleOptions,
  ScheduleRecord
} from "./client-options.js";
import { FerricStoreClientCore } from "./client-core.js";
import { okLike } from "./client-helpers.js";
import { append, appendBool, type CommandArgument } from "./internal.js";
import {
  validateScheduleCreate,
  validateScheduleFire,
  validateScheduleFireDue,
  validateScheduleID,
  validateScheduleList,
  validateScheduleStatus
} from "./schedule-request.js";
import {
  optionalScheduleRecord,
  scheduleFireDueResponse,
  scheduleFireResponse,
  scheduleRecordList,
  scheduleRecordResponse
} from "./schedule-response.js";

/** @internal Schedule management surface shared by the complete Flow client. */
export class FerricStoreScheduleClient extends FerricStoreClientCore {
  async scheduleCreate(id: string, options: ScheduleOptions): Promise<ScheduleRecord> {
    validateScheduleCreate(id, options);
    const args: CommandArgument[] = ["FLOW.SCHEDULE.CREATE", id];
    append(args, "KIND", options.kind);
    append(args, "AT_MS", options.atMs);
    append(args, "DELAY_MS", options.delayMs);
    append(args, "START_AT_MS", options.startAtMs);
    append(args, "EVERY_MS", options.everyMs);
    append(args, "CRON", options.cron);
    append(args, "TIMEZONE", options.timezone);
    append(args, "TARGET", options.target);
    append(args, "CATCHUP_POLICY", options.catchupPolicy);
    append(args, "OVERLAP_POLICY", options.overlapPolicy);
    append(args, "OVERLAP_RETRY_MS", options.overlapRetryMs);
    append(args, "MAX_FIRES", options.maxFires);
    append(args, "END_AT_MS", options.endAtMs);
    appendBool(args, "OVERWRITE", options.overwrite);
    append(args, "NOW", options.nowMs);
    for (const [name, value] of Object.entries(options.extraOptions ?? {})) {
      args.push(name.toUpperCase(), value);
    }
    return scheduleRecordResponse(await this.commandArgs(args), "FLOW.SCHEDULE.CREATE");
  }

  async scheduleGet(id: string): Promise<ScheduleRecord | null> {
    validateScheduleID(id);
    return optionalScheduleRecord(
      await this.commandArgs(["FLOW.SCHEDULE.GET", id]),
      "FLOW.SCHEDULE.GET"
    );
  }

  async scheduleFire(id: string, options: ScheduleFireOptions = {}): Promise<ScheduleFireResult> {
    validateScheduleFire(id, options);
    const args: CommandArgument[] = ["FLOW.SCHEDULE.FIRE", id];
    append(args, "FIRE_AT_MS", options.fireAtMs);
    append(args, "NOW", options.nowMs);
    return scheduleFireResponse(await this.commandArgs(args));
  }

  async schedulePause(id: string, options: { nowMs?: number } = {}): Promise<ScheduleRecord> {
    validateScheduleStatus(id, options.nowMs);
    return scheduleRecordResponse(
      await this.scheduleStatusResponse("FLOW.SCHEDULE.PAUSE", id, options.nowMs),
      "FLOW.SCHEDULE.PAUSE"
    );
  }

  async scheduleResume(id: string, options: { nowMs?: number } = {}): Promise<ScheduleRecord> {
    validateScheduleStatus(id, options.nowMs);
    return scheduleRecordResponse(
      await this.scheduleStatusResponse("FLOW.SCHEDULE.RESUME", id, options.nowMs),
      "FLOW.SCHEDULE.RESUME"
    );
  }

  async scheduleDelete(id: string, options: { nowMs?: number } = {}): Promise<void> {
    validateScheduleStatus(id, options.nowMs);
    const response = await this.scheduleStatusResponse("FLOW.SCHEDULE.DELETE", id, options.nowMs);
    if (!okLike(response)) {
      throw new TypeError("FLOW.SCHEDULE.DELETE response must be OK");
    }
  }

  async scheduleFireDue(options: ScheduleFireDueOptions = {}): Promise<ScheduleFireDueResult> {
    validateScheduleFireDue(options);
    const args: CommandArgument[] = ["FLOW.SCHEDULE.FIRE_DUE"];
    append(args, "NOW", options.nowMs);
    append(args, "WORKER", options.worker);
    append(args, "LEASE_MS", options.leaseMs);
    append(args, "BLOCK", options.blockMs);
    append(args, "LIMIT", options.limit);
    return scheduleFireDueResponse(await this.commandArgs(args));
  }

  async scheduleList(options: ScheduleListOptions = {}): Promise<ScheduleRecord[]> {
    validateScheduleList(options);
    const args: CommandArgument[] = ["FLOW.SCHEDULE.LIST"];
    append(args, "KIND", options.kind);
    append(args, "STATE", options.state);
    append(args, "TIMEZONE", options.timezone);
    append(args, "TARGET_TYPE", options.targetType);
    append(args, "FROM_MS", options.fromMs);
    append(args, "TO_MS", options.toMs);
    append(args, "COUNT", options.count);
    appendBool(args, "REV", options.rev);
    return scheduleRecordList(await this.commandArgs(args), "FLOW.SCHEDULE.LIST");
  }

  private async scheduleStatusResponse(
    command: string,
    id: string,
    now: number | undefined
  ): Promise<unknown> {
    const args: CommandArgument[] = [command, id];
    append(args, "NOW", now);
    return await this.commandArgs(args);
  }
}
