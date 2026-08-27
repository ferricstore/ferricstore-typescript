import net from "node:net";
import nativeProtocol from "../src/native-protocol-manifest.json" with { type: "json" };

const MAGIC = nativeProtocol.magic;
const REQUEST_VERSION = nativeProtocol.requestVersion;
const RESPONSE_VERSION = nativeProtocol.responseVersion;
const HEADER_SIZE = nativeProtocol.headerSize;
const OP_STARTUP = 0x000c;
const OP_PING = 0x0003;
const OP_COMMAND_EXEC = 0x0100;
const OP_CLUSTER_HEALTH = 0x0301;
const STATUS_OK = nativeProtocol.statusOk;
const REQUIRED_READY_SAMPLES = 3;

const host = process.env.FERRICSTORE_HOST ?? "127.0.0.1";
const port = Number(process.env.FERRICSTORE_PORT ?? "6388");
const deadline = Date.now() + Number(process.env.FERRICSTORE_WAIT_MS ?? "60000");
let readySamples = 0;

while (Date.now() < deadline) {
  if (await isNativeServerReady(host, port)) {
    readySamples += 1;
    if (readySamples >= REQUIRED_READY_SAMPLES) process.exit(0);
  } else {
    readySamples = 0;
  }
  await new Promise((resolve) => setTimeout(resolve, 500));
}

console.error(`Timed out waiting for FerricStore native protocol at ${host}:${port}`);
process.exit(1);

async function isNativeServerReady(targetHost, targetPort) {
  let socket;
  try {
    socket = await connect(targetHost, targetPort);
    const startup = await sendRequest(socket, OP_STARTUP, 1n, {
      client_name: "ferricstore-typescript-wait",
      compact_flow_responses: true,
      compact_response_codecs: ["flow_query_result_v1"],
      compression: "none",
      driver_name: "ferricstore-typescript-wait"
    });
    if (!supportsRequiredFlowQuery(startup)) {
      throw new Error("native startup capabilities are not ready");
    }
    await sendRequest(socket, OP_PING, 2n, {});
    await sendRequest(socket, OP_COMMAND_EXEC, 3n, {
      args: ["WHOAMI"],
      command: "ACL"
    });
    const clusterHealth = await sendRequest(socket, OP_CLUSTER_HEALTH, 4n, { args: [] });
    if (!clusterIsFullyReady(startup, clusterHealth)) {
      throw new Error("native cluster shards are not ready");
    }
    socket.destroy();
    return true;
  } catch {
    socket?.destroy();
    return false;
  }
}

function connect(targetHost, targetPort) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: targetHost, port: targetPort });
    socket.setNoDelay(true);
    socket.setTimeout(1000, () => {
      socket.destroy();
      reject(new Error("connect timeout"));
    });
    socket.once("connect", () => {
      socket.setTimeout(1000);
      resolve(socket);
    });
    socket.once("error", reject);
  });
}

async function sendRequest(socket, opcode, requestId, payload) {
  socket.write(encodeRequest(opcode, requestId, payload));
  const frame = await readFrame(socket);
  if (frame.opcode !== opcode || frame.requestId !== requestId) {
    throw new Error("unexpected native response");
  }
  if (frame.body.length < 2 || frame.body.readUInt16BE(0) !== STATUS_OK) {
    throw new Error("native response was not OK");
  }
  const decoded = decodeValue(frame.body, 2);
  if (decoded.offset !== frame.body.length) throw new Error("native response had trailing bytes");
  return decoded.value;
}

function supportsRequiredFlowQuery(startup) {
  const capabilities = field(startup, "capabilities") ?? startup;
  const flowQuery = field(capabilities, "flow_query");
  return textValue(field(flowQuery, "request_contract")) === "ferric.flow.query.request/v1"
    && textValue(field(flowQuery, "result_contract")) === "ferric.flow.query.result/v1";
}

function clusterIsFullyReady(startup, clusterHealth) {
  const shardCount = field(field(startup, "route"), "shard_count");
  const health = textValue(clusterHealth);
  if (!Number.isInteger(shardCount) || shardCount < 1 || health == null) return false;

  const statuses = health.match(/^\s*status:\s*ok\s*$/gim) ?? [];
  if (statuses.length !== shardCount) return false;
  for (let shard = 0; shard < shardCount; shard += 1) {
    if (!health.includes(`shard_${shard}:`)) return false;
  }
  return true;
}

function field(mapping, name) {
  return mapping != null && typeof mapping === "object" && !Array.isArray(mapping)
    ? mapping[name]
    : undefined;
}

function textValue(value) {
  if (Buffer.isBuffer(value)) return value.toString("utf8");
  return typeof value === "string" ? value : undefined;
}

function readFrame(socket) {
  return new Promise((resolve, reject) => {
    let buffer = Buffer.alloc(0);
    const cleanup = () => {
      socket.off("data", onData);
      socket.off("error", onError);
      socket.off("timeout", onTimeout);
      socket.off("close", onClose);
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    const onTimeout = () => {
      cleanup();
      reject(new Error("read timeout"));
    };
    const onClose = () => {
      cleanup();
      reject(new Error("connection closed"));
    };
    const onData = (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (buffer.length < HEADER_SIZE) return;
      if (buffer.toString("ascii", 0, 4) !== MAGIC || buffer.readUInt8(4) !== RESPONSE_VERSION) {
        cleanup();
        reject(new Error("invalid native response header"));
        return;
      }
      const bodyLength = buffer.readUInt32BE(20);
      const frameLength = HEADER_SIZE + bodyLength;
      if (buffer.length < frameLength) return;
      cleanup();
      resolve({
        body: buffer.subarray(HEADER_SIZE, frameLength),
        opcode: buffer.readUInt16BE(10),
        requestId: buffer.readBigUInt64BE(12)
      });
    };
    socket.on("data", onData);
    socket.once("error", onError);
    socket.once("timeout", onTimeout);
    socket.once("close", onClose);
  });
}

function encodeRequest(opcode, requestId, payload) {
  const body = encodeValue(payload);
  const frame = Buffer.allocUnsafe(HEADER_SIZE + body.length);
  frame.write(MAGIC, 0, "ascii");
  frame.writeUInt8(REQUEST_VERSION, 4);
  frame.writeUInt8(0, 5);
  frame.writeUInt32BE(laneForOpcode(opcode), 6);
  frame.writeUInt16BE(opcode, 10);
  frame.writeBigUInt64BE(requestId, 12);
  frame.writeUInt32BE(body.length, 20);
  body.copy(frame, HEADER_SIZE);
  return frame;
}

function encodeValue(value) {
  if (value == null) return Buffer.from([0]);
  if (value === true) return Buffer.from([1]);
  if (value === false) return Buffer.from([2]);
  if (typeof value === "string") return encodeBinary(Buffer.from(value));
  if (typeof value === "number") {
    const out = Buffer.allocUnsafe(9);
    out.writeUInt8(3, 0);
    out.writeBigInt64BE(BigInt(value), 1);
    return out;
  }
  if (Array.isArray(value)) return Buffer.concat([Buffer.from([5]), u32(value.length), ...value.map(encodeValue)]);
  const entries = Object.entries(value);
  return Buffer.concat([
    Buffer.from([6]),
    u32(entries.length),
    ...entries.flatMap(([key, item]) => [u32(Buffer.byteLength(key)), Buffer.from(key), encodeValue(item)])
  ]);
}

function decodeValue(data, offset, depth = 0) {
  if (depth > 64 || offset >= data.length) throw new Error("invalid native response value");
  const tag = data.readUInt8(offset++);
  if (tag === 0) return { offset, value: null };
  if (tag === 1) return { offset, value: true };
  if (tag === 2) return { offset, value: false };
  if (tag === 3 || tag === 8) {
    requireBytes(data, offset, 8);
    const integer = tag === 3 ? data.readBigInt64BE(offset) : data.readBigUInt64BE(offset);
    return {
      offset: offset + 8,
      value: integer <= BigInt(Number.MAX_SAFE_INTEGER) && integer >= BigInt(Number.MIN_SAFE_INTEGER)
        ? Number(integer)
        : integer
    };
  }
  if (tag === 4) return decodeBinary(data, offset);
  if (tag === 5) {
    const count = decodeCount(data, offset);
    offset += 4;
    const value = [];
    for (let index = 0; index < count; index += 1) {
      const decoded = decodeValue(data, offset, depth + 1);
      value.push(decoded.value);
      offset = decoded.offset;
    }
    return { offset, value };
  }
  if (tag === 6) {
    const count = decodeCount(data, offset);
    offset += 4;
    const value = Object.create(null);
    for (let index = 0; index < count; index += 1) {
      const key = decodeBinary(data, offset);
      const decoded = decodeValue(data, key.offset, depth + 1);
      value[key.value.toString("utf8")] = decoded.value;
      offset = decoded.offset;
    }
    return { offset, value };
  }
  if (tag === 7) {
    requireBytes(data, offset, 8);
    return { offset: offset + 8, value: data.readDoubleBE(offset) };
  }
  throw new Error(`unknown native response value tag ${tag}`);
}

function decodeBinary(data, offset) {
  requireBytes(data, offset, 4);
  const length = data.readUInt32BE(offset);
  offset += 4;
  requireBytes(data, offset, length);
  return { offset: offset + length, value: data.subarray(offset, offset + length) };
}

function decodeCount(data, offset) {
  requireBytes(data, offset, 4);
  const count = data.readUInt32BE(offset);
  if (count > 100_000) throw new Error("native response container is too large");
  return count;
}

function requireBytes(data, offset, length) {
  if (length < 0 || offset < 0 || offset + length > data.length) {
    throw new Error("truncated native response value");
  }
}

function encodeBinary(value) {
  return Buffer.concat([Buffer.from([4]), u32(value.length), value]);
}

function u32(value) {
  const out = Buffer.allocUnsafe(4);
  out.writeUInt32BE(value, 0);
  return out;
}

function laneForOpcode(opcode) {
  return opcode < OP_COMMAND_EXEC ? 0 : 1;
}
