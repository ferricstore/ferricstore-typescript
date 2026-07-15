import net from "node:net";
import nativeProtocol from "../src/native-protocol-manifest.json" with { type: "json" };

const MAGIC = nativeProtocol.magic;
const REQUEST_VERSION = nativeProtocol.requestVersion;
const RESPONSE_VERSION = nativeProtocol.responseVersion;
const HEADER_SIZE = nativeProtocol.headerSize;
const OP_STARTUP = 0x000c;
const OP_PING = 0x0003;
const STATUS_OK = nativeProtocol.statusOk;

const host = process.env.FERRICSTORE_HOST ?? "127.0.0.1";
const port = Number(process.env.FERRICSTORE_PORT ?? "6388");
const deadline = Date.now() + Number(process.env.FERRICSTORE_WAIT_MS ?? "60000");

while (Date.now() < deadline) {
  if (await canNativePing(host, port)) {
    process.exit(0);
  }
  await new Promise((resolve) => setTimeout(resolve, 500));
}

console.error(`Timed out waiting for FerricStore native protocol at ${host}:${port}`);
process.exit(1);

async function canNativePing(targetHost, targetPort) {
  let socket;
  try {
    socket = await connect(targetHost, targetPort);
    await sendRequest(socket, OP_STARTUP, 1n, {
      client_name: "ferricstore-typescript-wait",
      compact_flow_responses: true,
      compression: "none",
      driver_name: "ferricstore-typescript-wait"
    });
    await sendRequest(socket, OP_PING, 2n, {});
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
  frame.writeUInt32BE(0, 6);
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

function encodeBinary(value) {
  return Buffer.concat([Buffer.from([4]), u32(value.length), value]);
}

function u32(value) {
  const out = Buffer.allocUnsafe(4);
  out.writeUInt32BE(value, 0);
  return out;
}
