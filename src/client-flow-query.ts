import type { CommandArgument } from "./internal.js";
import type { ReadOptions, SearchOptions } from "./client-options.js";
import { FerricStoreError } from "./errors.js";
import {
  buildFlowFailureQuery,
  buildFlowLineageQuery,
  buildFlowListQuery,
  buildFlowSearchQuery,
  buildFlowStuckQuery,
  buildFlowTerminalQuery
} from "./flow-query-builder.js";
import {
  flowQueryArgs,
  hasFlowExplainPrefix,
  validateFlowQueryIndexId,
  validateFlowQueryText
} from "./flow-query-request.js";
import {
  decodeFlowExplainResult,
  decodeFlowQueryIndexStatus,
  decodeFlowQueryRecords,
  decodeFlowQueryResult,
  tryDecodeFlowQueryError
} from "./flow-query-response.js";
import type {
  FlowExplainResult,
  FlowQueryIndexStatus,
  FlowQueryParameters,
  FlowQueryResult
} from "./flow-query-types.js";
import { FerricStoreManagementClient } from "./client-management.js";
import { flowRecordFromResp, type FlowRecord } from "./types.js";

/** @internal Query-planner surface shared by the complete Flow client. */
export class FerricStoreFlowQueryClient extends FerricStoreManagementClient {
  async query(query: string, params: FlowQueryParameters = {}): Promise<FlowQueryResult> {
    if (hasFlowExplainPrefix(query)) {
      throw new TypeError("query does not accept EXPLAIN; use explain or explainAnalyze");
    }
    return decodeFlowQueryResult(await this.executeFlowQuery(query, params));
  }

  async explain(query: string, params: FlowQueryParameters = {}): Promise<FlowExplainResult> {
    return await this.executeExplain("EXPLAIN ", query, params);
  }

  async explainAnalyze(
    query: string,
    params: FlowQueryParameters = {}
  ): Promise<FlowExplainResult> {
    return await this.executeExplain("EXPLAIN ANALYZE ", query, params);
  }

  async queryIndexes(indexId?: string): Promise<FlowQueryIndexStatus> {
    const args: CommandArgument[] = ["FLOW.QUERY.INDEXES"];
    if (indexId != null) {
      validateFlowQueryIndexId(indexId);
      args.push(indexId);
    }
    try {
      return decodeFlowQueryIndexStatus(await this.commandArgs(args), indexId);
    } catch (error) {
      this.throwFlowQueryError(error);
    }
  }

  async list(type: string, options: ReadOptions): Promise<FlowRecord[]> {
    return await this.executeFlowRecordQuery(buildFlowListQuery(type, options));
  }

  async search(type: string, options: SearchOptions): Promise<FlowRecord[]> {
    return await this.executeFlowRecordQuery(buildFlowSearchQuery(type, options));
  }

  async terminals(type: string, options: ReadOptions): Promise<FlowRecord[]> {
    return await this.executeFlowRecordQuery(buildFlowTerminalQuery(type, options));
  }

  async failures(type: string, options: ReadOptions): Promise<FlowRecord[]> {
    return await this.executeFlowRecordQuery(buildFlowFailureQuery(type, options));
  }

  async byParent(parentFlowId: string, options: ReadOptions): Promise<FlowRecord[]> {
    return await this.executeFlowRecordQuery(
      buildFlowLineageQuery("parent_flow_id", parentFlowId, options)
    );
  }

  async byRoot(rootFlowId: string, options: ReadOptions): Promise<FlowRecord[]> {
    return await this.executeFlowRecordQuery(
      buildFlowLineageQuery("root_flow_id", rootFlowId, options)
    );
  }

  async byCorrelation(correlationId: string, options: ReadOptions): Promise<FlowRecord[]> {
    return await this.executeFlowRecordQuery(
      buildFlowLineageQuery("correlation_id", correlationId, options)
    );
  }

  async stuck(type: string, options: {
    partitionKey: string;
    count?: number;
    olderThanMs?: number;
    nowMs?: number;
  }): Promise<FlowRecord[]> {
    return await this.executeFlowRecordQuery(buildFlowStuckQuery(type, options));
  }

  protected records(values: unknown[]): FlowRecord[] {
    const records = new Array<FlowRecord>(values.length);
    for (let index = 0; index < values.length; index += 1) {
      if (!Object.hasOwn(values, index)) {
        throw new TypeError(`Flow record response item ${index} is missing`);
      }
      records[index] = flowRecordFromResp(values[index], this.codec);
    }
    return records;
  }

  private async executeFlowQuery(query: string, params: FlowQueryParameters): Promise<unknown> {
    const args = flowQueryArgs(query, params);
    try {
      return await this.commandArgs(args);
    } catch (error) {
      this.throwFlowQueryError(error);
    }
  }

  private async executeFlowRecordQuery(request: {
    readonly query: string;
    readonly params: FlowQueryParameters;
  }): Promise<FlowRecord[]> {
    const response = await this.executeFlowQuery(request.query, request.params);
    return decodeFlowQueryRecords(response, (value) => flowRecordFromResp(value, this.codec));
  }

  private async executeExplain(
    prefix: "EXPLAIN " | "EXPLAIN ANALYZE ",
    query: string,
    params: FlowQueryParameters
  ): Promise<FlowExplainResult> {
    validateFlowQueryText(query);
    if (hasFlowExplainPrefix(query)) {
      throw new TypeError("query already contains an EXPLAIN prefix");
    }
    return decodeFlowExplainResult(await this.executeFlowQuery(prefix + query, params));
  }

  private throwFlowQueryError(error: unknown): never {
    if (error instanceof FerricStoreError) {
      const diagnostic = tryDecodeFlowQueryError(error.raw, error);
      if (diagnostic != null) throw diagnostic;
    }
    throw error;
  }
}
