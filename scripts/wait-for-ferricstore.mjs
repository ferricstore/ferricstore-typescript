import net from "node:net";

const host = process.env.FERRICSTORE_HOST ?? "127.0.0.1";
const port = Number(process.env.FERRICSTORE_PORT ?? "6379");
const deadline = Date.now() + Number(process.env.FERRICSTORE_WAIT_MS ?? "60000");

while (Date.now() < deadline) {
  if (await canConnect(host, port)) {
    process.exit(0);
  }
  await new Promise((resolve) => setTimeout(resolve, 500));
}

console.error(`Timed out waiting for FerricStore at ${host}:${port}`);
process.exit(1);

function canConnect(targetHost, targetPort) {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host: targetHost, port: targetPort });
    socket.once("connect", () => {
      socket.destroy();
      resolve(true);
    });
    socket.once("error", () => {
      socket.destroy();
      resolve(false);
    });
    socket.setTimeout(500, () => {
      socket.destroy();
      resolve(false);
    });
  });
}
