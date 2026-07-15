import { FerricStoreClientBase } from "./client-base.js";
import { concatCommandArgs } from "./client-core-helpers.js";
import {
  commandWithRequestContext,
  jsonArg,
  managementPairArgs,
  normalizeAdminResponse
} from "./client-helpers.js";
import type {
  InvocationCreateOptions,
  ManagementPairs,
  RequestContextOptions
} from "./client-options.js";
import {
  append,
  arrayResponse,
  integer,
  okResponse,
  parseKvResponse,
  textResponse,
  type CommandArgument
} from "./internal.js";

/** Cluster, FerricStore administration, telemetry, and invocation commands. */
export class FerricStoreAdministrationClient extends FerricStoreClientBase {
  async clusterHealth(): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("CLUSTER.HEALTH"));
  }

  async clusterStats(): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("CLUSTER.STATS"));
  }

  async clusterKeyslot(key: string): Promise<number> {
    return integer(await this.command("CLUSTER.KEYSLOT", key));
  }

  async clusterSlots(): Promise<unknown> {
    return await this.command("CLUSTER.SLOTS");
  }

  async clusterStatus(): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.command("CLUSTER.STATUS"));
  }

  async clusterRole(): Promise<unknown> {
    return await this.command("CLUSTER.ROLE");
  }

  async clusterJoin(node: string, options: { replace?: boolean } = {}): Promise<boolean> {
    return okResponse(await this.command(
      "CLUSTER.JOIN",
      node,
      ...(options.replace === true ? ["REPLACE"] : [])
    ));
  }

  async clusterLeave(): Promise<boolean> {
    return okResponse(await this.command("CLUSTER.LEAVE"));
  }

  async clusterFailover(shardIndex: number, targetNode: string): Promise<boolean> {
    return okResponse(await this.command("CLUSTER.FAILOVER", shardIndex, targetNode));
  }

  async clusterPromote(node: string): Promise<boolean> {
    return okResponse(await this.command("CLUSTER.PROMOTE", node));
  }

  async clusterDemote(node: string): Promise<boolean> {
    return okResponse(await this.command("CLUSTER.DEMOTE", node));
  }

  async ferricstoreConfig(...args: CommandArgument[]): Promise<unknown> {
    return await this.commandArgs(concatCommandArgs(["FERRICSTORE.CONFIG"], args));
  }

  async ferricstoreHotness(...args: CommandArgument[]): Promise<Record<string, unknown>> {
    return parseKvResponse(await this.commandArgs(concatCommandArgs(["FERRICSTORE.HOTNESS"], args)));
  }

  async ferricstoreMetrics(...args: CommandArgument[]): Promise<string> {
    return textResponse(
      await this.commandArgs(concatCommandArgs(["FERRICSTORE.METRICS"], args)),
      "FERRICSTORE.METRICS"
    );
  }

  async ferricstoreBlobgc(...args: CommandArgument[]): Promise<unknown> {
    return await this.commandArgs(concatCommandArgs(["FERRICSTORE.BLOBGC"], args));
  }

  async ferricstoreDoctor(...args: CommandArgument[]): Promise<unknown> {
    return await this.commandArgs(concatCommandArgs(["FERRICSTORE.DOCTOR"], args));
  }

  async capabilities(): Promise<Record<string, unknown>> {
    return normalizeAdminResponse(
      await this.command("FERRICSTORE.CAPABILITIES")
    ) as Record<string, unknown>;
  }

  async ensureNamespace(prefix: string, attrs: ManagementPairs = {}): Promise<unknown> {
    return normalizeAdminResponse(
      await this.commandArgs(
        concatCommandArgs(["FERRICSTORE.NAMESPACE", "ENSURE", prefix], managementPairArgs(attrs))
      )
    );
  }

  async getNamespace(prefix: string): Promise<unknown> {
    return normalizeAdminResponse(await this.command("FERRICSTORE.NAMESPACE", "GET", prefix));
  }

  async listNamespaces(): Promise<unknown> {
    return normalizeAdminResponse(await this.command("FERRICSTORE.NAMESPACE", "LIST"));
  }

  async deleteNamespace(prefix: string): Promise<unknown> {
    return normalizeAdminResponse(await this.command("FERRICSTORE.NAMESPACE", "DELETE", prefix));
  }

  async setQuota(namespace: string, quotaSpec: ManagementPairs = {}): Promise<unknown> {
    return normalizeAdminResponse(
      await this.commandArgs(
        concatCommandArgs(["FERRICSTORE.QUOTA", "SET", namespace], managementPairArgs(quotaSpec))
      )
    );
  }

  async getQuota(namespace: string): Promise<unknown> {
    return normalizeAdminResponse(await this.command("FERRICSTORE.QUOTA", "GET", namespace));
  }

  async quotaUsage(namespace: string): Promise<unknown> {
    return normalizeAdminResponse(await this.command("FERRICSTORE.QUOTA", "USAGE", namespace));
  }

  async clusterInfo(): Promise<Record<string, unknown>> {
    return normalizeAdminResponse(
      await this.command("FERRICSTORE.TELEMETRY", "CLUSTER_INFO")
    ) as Record<string, unknown>;
  }

  async namespaceUsage(prefix: string): Promise<Record<string, unknown>> {
    return normalizeAdminResponse(
      await this.command("FERRICSTORE.TELEMETRY", "NAMESPACE_USAGE", prefix)
    ) as Record<string, unknown>;
  }

  async flowQuery(attrs: ManagementPairs = {}): Promise<unknown[]> {
    const response = normalizeAdminResponse(
      await this.commandArgs(
        concatCommandArgs(["FERRICSTORE.TELEMETRY", "FLOW_QUERY"], managementPairArgs(attrs))
      )
    );
    return arrayResponse(response);
  }

  async flowHistory(id: string, attrs: ManagementPairs = {}): Promise<unknown[]> {
    const response = normalizeAdminResponse(
      await this.commandArgs(
        concatCommandArgs(
          ["FERRICSTORE.TELEMETRY", "FLOW_HISTORY", id],
          managementPairArgs(attrs)
        )
      )
    );
    return arrayResponse(response);
  }

  async invocationDefinitionPut(
    definition: Record<string, unknown> | string,
    options: RequestContextOptions = {}
  ): Promise<unknown> {
    return normalizeAdminResponse(
      await this.commandArgs(commandWithRequestContext(
        "INVOCATION.DEFINITION.PUT",
        [jsonArg(definition)],
        options.requestContext
      ))
    );
  }

  async invocationDefinitionGet(
    name: string,
    options: RequestContextOptions = {}
  ): Promise<unknown> {
    return normalizeAdminResponse(
      await this.commandArgs(commandWithRequestContext(
        "INVOCATION.DEFINITION.GET",
        [name],
        options.requestContext
      ))
    );
  }

  async invocationDefinitionList(options: RequestContextOptions = {}): Promise<unknown[]> {
    const response = normalizeAdminResponse(
      await this.commandArgs(commandWithRequestContext(
        "INVOCATION.DEFINITION.LIST",
        [],
        options.requestContext
      ))
    );
    return arrayResponse(response);
  }

  async invocationCreate(
    name: string,
    attrs: Record<string, unknown>,
    options: InvocationCreateOptions = {}
  ): Promise<unknown> {
    const envelope: Record<string, unknown> = { attrs };
    if (options.context != null) envelope.context = options.context;
    if (options.idempotencyKey != null) envelope.idempotency_key = options.idempotencyKey;
    return normalizeAdminResponse(
      await this.commandArgs(commandWithRequestContext(
        "INVOCATION.CREATE",
        [name, jsonArg(envelope)],
        options.requestContext
      ))
    );
  }

  async invocationGet(id: string, options: RequestContextOptions = {}): Promise<unknown> {
    return normalizeAdminResponse(
      await this.commandArgs(commandWithRequestContext(
        "INVOCATION.GET",
        [id],
        options.requestContext
      ))
    );
  }

  async invocationPartitionList(
    name: string,
    options: RequestContextOptions & { scope?: string } = {}
  ): Promise<unknown[]> {
    const args: CommandArgument[] = [name];
    append(args, "SCOPE", options.scope);
    const response = normalizeAdminResponse(
      await this.commandArgs(commandWithRequestContext(
        "INVOCATION.PARTITION.LIST",
        args,
        options.requestContext
      ))
    );
    return arrayResponse(response);
  }
}
