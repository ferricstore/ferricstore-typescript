// Fan-out and fan-in: an image Flow starts one resize child per size, waits
// for all of them without holding a worker, then reads their results.
import { fileURLToPath } from "node:url";
import {
  FerricStoreClient,
  JsonCodec,
  WorkflowClient,
  complete,
  fail,
  waitForChildren,
  type FlowRecord
} from "../src/index.js";
import { runUntilTerminal } from "./drive-workers.js";

export const SIZES = ["small", "large"] as const;

interface ResizeJob {
  imageId: string;
  size: string;
}

export function defineImageWorkflows(workflows: WorkflowClient) {
  const image = workflows.workflow({ initialState: "received", type: "image", worker: "image-worker-1" });
  const resize = workflows.workflow({ initialState: "queued", type: "resize", worker: "resize-worker-1" });

  // Creating the children and parking the parent in "waiting_for_resizes" is
  // one fenced command. The parent holds no lease while it waits.
  image.state("received", (ctx) =>
    waitForChildren(
      SIZES.map((size) => ({ id: `${ctx.id}:${size}`, payload: { imageId: ctx.id, size }, type: "resize" })),
      { failure: "resize_failed", groupId: "resizes", success: "resized", waitState: "waiting_for_resizes" }
    )
  );

  // Each child is an ordinary Flow with its own lease, retries, and history.
  resize.state("queued", (ctx) => {
    const { imageId, size } = ctx.payload as ResizeJob;
    return complete({ result: { size, url: `s3://images/${imageId}/${size}.jpg` } });
  });

  // FerricStore moves the parent here once every child has completed.
  image.state("resized", async (ctx) => {
    const resizes = await ctx.flow.childResults<{ size: string; url: string }>("resizes");
    return complete({ result: { urls: resizes.children.map((child) => child.result?.url) } });
  });

  // With the default onChildFailed: "fail_parent", the first failed child
  // moves the parent here and cancels the children still running.
  image.state("resize_failed", async (ctx) => {
    const resizes = await ctx.flow.childResults("resizes");
    const failed = resizes.children.filter((child) => child.status === "failed").map((child) => child.id);
    return fail({ error: { failed, message: "resize failed" } });
  });

  return { image, resize };
}

export async function processImage(flow: FerricStoreClient, imageId: string): Promise<FlowRecord> {
  const { image, resize } = defineImageWorkflows(new WorkflowClient(flow));
  await image.start(imageId, { idempotent: true, payload: { uploadId: "upload-1" } });
  return await runUntilTerminal(flow, imageId, [
    image.worker({ states: ["received", "resized", "resize_failed"] }),
    resize.worker({ states: ["queued"] })
  ]);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const flow = await FerricStoreClient.fromUrl(process.env.FERRICSTORE_URL ?? "ferric://127.0.0.1:6388", {
    codec: new JsonCodec()
  });
  const record = await processImage(flow, "image-1");
  console.log(record.state, record.result);
  await flow.close();
}
