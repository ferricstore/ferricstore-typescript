import {
  append,
  appendBool,
  integer,
  type CommandArgument
} from "./internal.js";
import type {
  AdminListOptions,
  ApprovalListOptions,
  ApprovalRequestOptions,
  AttributeQueryOptions,
  BudgetCommitOptions,
  BudgetReserveOptions,
  CircuitOpenOptions,
  EffectCompensateOptions,
  EffectConfirmOptions,
  EffectFailOptions,
  EffectReserveOptions,
  EffectStatusOptions,
  FlowAdminRecord,
  FlowStatsOptions,
  GovernanceLedgerOptions,
  LimitAmountOptions,
  LimitLeaseOptions,
  LimitListOptions,
  LimitReleaseOptions,
  ScheduleFireDueOptions,
  ScheduleListOptions,
  ScheduleOptions
} from "./client-options.js";
import {
  adminListArgs,
  adminRecordList,
  adminRecordResponse,
  appendAttributeQueryOptions,
  appendAttributes,
  approvalListArgs,
  okLike,
  optionalAdminRecord
} from "./client-helpers.js";
import { FerricStoreClientCore } from "./client-core.js";
import { snapshotOwnStringArray } from "./string-array-snapshot.js";

/** @internal Administrative and governance commands kept off the primary Flow client surface. */
export class FerricStoreManagementClient extends FerricStoreClientCore {
  async stats(type: string, options: FlowStatsOptions = {}): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = ["FLOW.STATS", type];
    append(args, "STATE", options.state);
    append(args, "COUNT", options.count);
    append(args, "PARTITION", options.partitionKey);
    appendAttributes(args, options.attributes);
    appendBool(args, "CONSISTENT_PROJECTION", options.consistentProjection);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.STATS");
  }

  async countByState(
    type: string,
    state: string,
    options: Omit<FlowStatsOptions, "state" | "count"> = {}
  ): Promise<number> {
    const stats = await this.stats(type, { ...options, state });
    if (!("count" in stats)) throw new TypeError("FLOW.STATS response missing count");
    return integer(stats.count);
  }

  async attributes(type: string, options: AttributeQueryOptions = {}): Promise<FlowAdminRecord[]> {
    const args: CommandArgument[] = ["FLOW.ATTRIBUTES", type];
    appendAttributeQueryOptions(args, options);
    return adminRecordList(await this.commandArgs(args), "FLOW.ATTRIBUTES");
  }

  async attributeValues(
    type: string,
    attribute: string,
    options: AttributeQueryOptions = {}
  ): Promise<FlowAdminRecord[]> {
    const args: CommandArgument[] = ["FLOW.ATTRIBUTE_VALUES", type, attribute];
    appendAttributeQueryOptions(args, options);
    return adminRecordList(await this.commandArgs(args), "FLOW.ATTRIBUTE_VALUES");
  }

  async scheduleCreate(id: string, options: ScheduleOptions): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = ["FLOW.SCHEDULE.CREATE", id];
    append(args, "KIND", options.kind);
    append(args, "AT_MS", options.atMs);
    append(args, "DELAY_MS", options.delayMs);
    append(args, "START_AT_MS", options.startAtMs);
    append(args, "EVERY_MS", options.everyMs);
    append(args, "CRON", options.cron);
    append(args, "TIMEZONE", options.timezone);
    append(args, "TARGET", options.target);
    append(args, "OVERLAP_POLICY", options.overlapPolicy);
    append(args, "OVERLAP_RETRY_MS", options.overlapRetryMs);
    append(args, "MAX_FIRES", options.maxFires);
    append(args, "END_AT_MS", options.endAtMs);
    appendBool(args, "OVERWRITE", options.overwrite);
    append(args, "NOW", options.nowMs);
    for (const [name, value] of Object.entries(options.extraOptions ?? {})) {
      args.push(name.toUpperCase(), value);
    }
    return adminRecordResponse(await this.commandArgs(args), "FLOW.SCHEDULE.CREATE");
  }

  async scheduleGet(id: string, options: { nowMs?: number } = {}): Promise<FlowAdminRecord | null> {
    const args: CommandArgument[] = ["FLOW.SCHEDULE.GET", id];
    append(args, "NOW", options.nowMs);
    return optionalAdminRecord(await this.commandArgs(args), "FLOW.SCHEDULE.GET");
  }

  async scheduleFire(id: string, options: { nowMs?: number } = {}): Promise<FlowAdminRecord> {
    return await this.scheduleStatus("FLOW.SCHEDULE.FIRE", id, options.nowMs);
  }

  async schedulePause(id: string, options: { nowMs?: number } = {}): Promise<FlowAdminRecord> {
    return await this.scheduleStatus("FLOW.SCHEDULE.PAUSE", id, options.nowMs);
  }

  async scheduleResume(id: string, options: { nowMs?: number } = {}): Promise<FlowAdminRecord> {
    return await this.scheduleStatus("FLOW.SCHEDULE.RESUME", id, options.nowMs);
  }

  async scheduleDelete(id: string, options: { nowMs?: number } = {}): Promise<FlowAdminRecord> {
    return await this.scheduleStatus("FLOW.SCHEDULE.DELETE", id, options.nowMs);
  }

  async scheduleFireDue(options: ScheduleFireDueOptions = {}): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = ["FLOW.SCHEDULE.FIRE_DUE"];
    append(args, "NOW", options.nowMs);
    append(args, "WORKER", options.worker);
    append(args, "BLOCK", options.blockMs);
    append(args, "LIMIT", options.limit);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.SCHEDULE.FIRE_DUE");
  }

  async scheduleList(options: ScheduleListOptions = {}): Promise<FlowAdminRecord[]> {
    const args: CommandArgument[] = ["FLOW.SCHEDULE.LIST"];
    append(args, "KIND", options.kind);
    append(args, "STATE", options.state);
    append(args, "TIMEZONE", options.timezone);
    append(args, "TARGET_TYPE", options.targetType);
    append(args, "FROM_MS", options.fromMs);
    append(args, "TO_MS", options.toMs);
    append(args, "COUNT", options.count);
    appendBool(args, "REV", options.rev);
    return adminRecordList(await this.commandArgs(args), "FLOW.SCHEDULE.LIST");
  }

  private async scheduleStatus(command: string, id: string, now: number | undefined): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = [command, id];
    append(args, "NOW", now);
    const response = await this.commandArgs(args);
    if (okLike(response)) return { id, status: "deleted" };
    return adminRecordResponse(response, command);
  }

  async effectReserve(
    id: string,
    effectKey: string,
    effectType: string,
    options: EffectReserveOptions
  ): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = [
      "FLOW.EFFECT.RESERVE", id, "EFFECT_KEY", effectKey, "EFFECT_TYPE", effectType
    ];
    append(args, "PARTITION", options.partitionKey);
    append(args, "LEASE_TOKEN", options.leaseToken);
    append(args, "FENCING", options.fencingToken);
    append(args, "OPERATION_DIGEST", options.operationDigest);
    append(args, "IDEMPOTENCY_KEY", options.idempotencyKey);
    append(args, "GOVERNANCE_SCOPE", options.governanceScope);
    append(args, "NOW", options.nowMs);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.EFFECT.RESERVE");
  }

  async effectConfirm(id: string, effectKey: string, options: EffectConfirmOptions = {}): Promise<FlowAdminRecord> {
    return await this.effectStatus("FLOW.EFFECT.CONFIRM", id, effectKey, options);
  }

  async effectFail(id: string, effectKey: string, options: EffectFailOptions = {}): Promise<FlowAdminRecord> {
    return await this.effectStatus("FLOW.EFFECT.FAIL", id, effectKey, options);
  }

  async effectCompensate(
    id: string,
    effectKey: string,
    options: EffectCompensateOptions = {}
  ): Promise<FlowAdminRecord> {
    return await this.effectStatus("FLOW.EFFECT.COMPENSATE", id, effectKey, options);
  }

  async effectGet(
    id: string,
    effectKey: string,
    options: { partitionKey?: string } = {}
  ): Promise<FlowAdminRecord | null> {
    const args: CommandArgument[] = ["FLOW.EFFECT.GET", id, "EFFECT_KEY", effectKey];
    append(args, "PARTITION", options.partitionKey);
    return optionalAdminRecord(await this.commandArgs(args), "FLOW.EFFECT.GET");
  }

  private async effectStatus(
    command: string,
    id: string,
    effectKey: string,
    options: EffectStatusOptions & {
      externalId?: string;
      error?: string;
      reason?: string;
      latencyMs?: number;
    }
  ): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = [command, id, "EFFECT_KEY", effectKey];
    append(args, "PARTITION", options.partitionKey);
    append(args, "LEASE_TOKEN", options.leaseToken);
    append(args, "FENCING", options.fencingToken);
    append(args, "EXTERNAL_ID", options.externalId);
    append(args, "ERROR", options.error);
    append(args, "REASON", options.reason);
    append(args, "LATENCY_MS", options.latencyMs);
    append(args, "NOW", options.nowMs);
    return adminRecordResponse(await this.commandArgs(args), command);
  }

  async approvalRequest(id: string, options: ApprovalRequestOptions): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = [
      "FLOW.APPROVAL.REQUEST", id, "FLOW_ID", options.flowId, "SCOPE", options.scope
    ];
    append(args, "REASON", options.reason);
    append(args, "REQUESTED_BY", options.requestedBy);
    append(args, "ASSIGNEES", options.assignees);
    append(args, "POLICY_HASH", options.policyHash);
    append(args, "POLICY_VERSION", options.policyVersion);
    append(args, "TIMEOUT_MS", options.timeoutMs);
    append(args, "EXPIRES_AT_MS", options.expiresAtMs);
    append(args, "NOW", options.nowMs);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.APPROVAL.REQUEST");
  }

  async approvalApprove(
    id: string,
    options: { approver: string; reason?: string; nowMs?: number }
  ): Promise<FlowAdminRecord> {
    return await this.approvalStatus("FLOW.APPROVAL.APPROVE", id, options);
  }

  async approvalReject(
    id: string,
    options: { approver: string; reason?: string; nowMs?: number }
  ): Promise<FlowAdminRecord> {
    return await this.approvalStatus("FLOW.APPROVAL.REJECT", id, options);
  }

  private async approvalStatus(
    command: string,
    id: string,
    options: { approver: string; reason?: string; nowMs?: number }
  ): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = [command, id, "APPROVER", options.approver];
    append(args, "REASON", options.reason);
    append(args, "NOW", options.nowMs);
    return adminRecordResponse(await this.commandArgs(args), command);
  }

  async approvalGet(id: string): Promise<FlowAdminRecord | null> {
    return optionalAdminRecord(await this.command("FLOW.APPROVAL.GET", id), "FLOW.APPROVAL.GET");
  }

  async approvalList(options: ApprovalListOptions = {}): Promise<FlowAdminRecord[]> {
    const args = approvalListArgs("FLOW.APPROVAL.LIST", options);
    return adminRecordList(await this.commandArgs(args), "FLOW.APPROVAL.LIST");
  }

  async governanceOverview(options: ApprovalListOptions = {}): Promise<FlowAdminRecord> {
    const args = approvalListArgs("FLOW.GOVERNANCE.OVERVIEW", options);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.GOVERNANCE.OVERVIEW");
  }

  async circuitOpen(scope: string, options: CircuitOpenOptions = {}): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = ["FLOW.CIRCUIT.OPEN", scope];
    append(args, "OPEN_MS", options.openMs);
    append(args, "FAILURE_THRESHOLD", options.failureThreshold);
    append(args, "NOW", options.nowMs);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.CIRCUIT.OPEN");
  }

  async circuitClose(scope: string, options: { nowMs?: number } = {}): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = ["FLOW.CIRCUIT.CLOSE", scope];
    append(args, "NOW", options.nowMs);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.CIRCUIT.CLOSE");
  }

  async circuitGet(scope: string): Promise<FlowAdminRecord | null> {
    return optionalAdminRecord(await this.command("FLOW.CIRCUIT.GET", scope), "FLOW.CIRCUIT.GET");
  }

  async budgetReserve(
    scope: string,
    amount: number,
    options: BudgetReserveOptions = {}
  ): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = ["FLOW.BUDGET.RESERVE", scope, "AMOUNT", amount];
    append(args, "LIMIT", options.limit);
    append(args, "WINDOW_MS", options.windowMs);
    append(args, "RESERVATION_ID", options.reservationId);
    append(args, "NOW", options.nowMs);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.BUDGET.RESERVE");
  }

  async budgetCommit(
    scope: string,
    reservationId: string,
    actualAmount: number,
    options: BudgetCommitOptions = {}
  ): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = [
      "FLOW.BUDGET.COMMIT", scope, "RESERVATION_ID", reservationId, "ACTUAL_AMOUNT", actualAmount
    ];
    append(args, "USAGE", options.usage);
    append(args, "NOW", options.nowMs);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.BUDGET.COMMIT");
  }

  async budgetRelease(
    scope: string,
    reservationId: string,
    options: { nowMs?: number } = {}
  ): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = ["FLOW.BUDGET.RELEASE", scope, "RESERVATION_ID", reservationId];
    append(args, "NOW", options.nowMs);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.BUDGET.RELEASE");
  }

  async budgetGet(scope: string): Promise<FlowAdminRecord | null> {
    return optionalAdminRecord(await this.command("FLOW.BUDGET.GET", scope), "FLOW.BUDGET.GET");
  }

  async budgetList(options: AdminListOptions = {}): Promise<FlowAdminRecord[]> {
    const args = adminListArgs("FLOW.BUDGET.LIST", options);
    return adminRecordList(await this.commandArgs(args), "FLOW.BUDGET.LIST");
  }

  async limitLease(scope: string, options: LimitLeaseOptions): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = [
      "FLOW.LIMIT.LEASE", scope,
      "SHARD_ID", options.shardId,
      "AMOUNT", options.amount,
      "TTL_MS", options.ttlMs
    ];
    append(args, "LIMIT", options.limit);
    append(args, "NOW", options.nowMs);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.LIMIT.LEASE");
  }

  async limitSpend(scope: string, options: LimitAmountOptions): Promise<FlowAdminRecord> {
    const args: CommandArgument[] = [
      "FLOW.LIMIT.SPEND", scope, "SHARD_ID", options.shardId, "AMOUNT", options.amount
    ];
    append(args, "NOW", options.nowMs);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.LIMIT.SPEND");
  }

  async limitRelease(
    scope: string,
    options: LimitReleaseOptions
  ): Promise<FlowAdminRecord> {
    const reservationIds = snapshotOwnStringArray(
      options.reservationIds,
      "limitRelease reservationIds"
    );
    const seenReservationIds = new Set<string>();
    for (const reservationId of reservationIds) {
      if (
        typeof reservationId !== "string" ||
        reservationId.length === 0 ||
        seenReservationIds.has(reservationId)
      ) {
        throw new TypeError("limitRelease reservationIds must contain one unique non-empty id per credit");
      }
      seenReservationIds.add(reservationId);
    }
    if (reservationIds.length === 0) {
      throw new TypeError("limitRelease reservationIds must contain one unique non-empty id per credit");
    }
    if (
      options.amount != null &&
      (!Number.isSafeInteger(options.amount) || options.amount <= 0 || options.amount !== reservationIds.length)
    ) {
      throw new TypeError("limitRelease amount must match reservationIds.length");
    }
    const args: CommandArgument[] = [
      "FLOW.LIMIT.RELEASE", scope, "SHARD_ID", options.shardId
    ];
    append(args, "AMOUNT", options.amount);
    args.push("RESERVATION_IDS", reservationIds.length);
    for (const reservationId of reservationIds) args.push(reservationId);
    return adminRecordResponse(await this.commandArgs(args), "FLOW.LIMIT.RELEASE");
  }

  async limitGet(scope: string, options: { nowMs?: number } = {}): Promise<FlowAdminRecord | null> {
    const args: CommandArgument[] = ["FLOW.LIMIT.GET", scope];
    append(args, "NOW", options.nowMs);
    return optionalAdminRecord(await this.commandArgs(args), "FLOW.LIMIT.GET");
  }

  async limitList(options: LimitListOptions = {}): Promise<FlowAdminRecord[]> {
    const args = adminListArgs("FLOW.LIMIT.LIST", options);
    return adminRecordList(await this.commandArgs(args), "FLOW.LIMIT.LIST");
  }

  async governanceLedger(id: string, options: GovernanceLedgerOptions = {}): Promise<FlowAdminRecord[]> {
    const args: CommandArgument[] = ["FLOW.GOVERNANCE.LEDGER", id];
    append(args, "PARTITION", options.partitionKey);
    append(args, "LIMIT", options.limit);
    append(args, "FROM_MS", options.fromMs);
    append(args, "TO_MS", options.toMs);
    appendBool(args, "REV", options.rev);
    return adminRecordList(await this.commandArgs(args), "FLOW.GOVERNANCE.LEDGER");
  }
}
