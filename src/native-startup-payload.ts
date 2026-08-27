export function nativeStartupPayload(
  clientName?: string,
  events?: readonly string[]
): Record<string, unknown> {
  const name = clientName ?? "ferricstore-typescript";
  return {
    client_name: name,
    compact_flow_responses: true,
    compact_response_codecs: ["flow_query_result_v1"],
    compression: "none",
    driver_name: name,
    ...(events == null || events.length === 0 ? {} : { events: [...events] })
  };
}
