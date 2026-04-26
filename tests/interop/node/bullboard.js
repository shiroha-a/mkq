// bull-board admin server, used by the Go interop tests to prove
// mkq's wire state renders correctly under BullMQ's canonical admin UI.
//
// Env:
//   INTEROP_REDIS              host:port
//   INTEROP_PREFIX             BullMQ keyPrefix
//   INTEROP_QUEUE              queue name
//   INTEROP_BULLBOARD_PORT     listen port (default 0 = let OS pick)
//
// Output:
//   single line of JSON on stdout once Express is listening:
//     {"event":"ready","port":N}
//   thereafter stdout is drained but otherwise silent.
//
// Exits cleanly on SIGTERM / SIGINT.

import express from "express";
import { ExpressAdapter } from "@bull-board/express";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { Queue } from "bullmq";

const redisAddr = process.env.INTEROP_REDIS;
const prefix    = process.env.INTEROP_PREFIX;
const queueName = process.env.INTEROP_QUEUE;
const portReq   = Number(process.env.INTEROP_BULLBOARD_PORT || "0");

if (!redisAddr || !prefix || !queueName) {
  console.error("missing env: INTEROP_REDIS / INTEROP_PREFIX / INTEROP_QUEUE");
  process.exit(2);
}

const [host, redisPortStr] = redisAddr.split(":");
const redisPort = Number(redisPortStr);

const queue = new Queue(queueName, {
  connection: { host, port: redisPort },
  prefix,
});

const adapter = new ExpressAdapter();
adapter.setBasePath("/admin");
createBullBoard({
  queues: [new BullMQAdapter(queue)],
  serverAdapter: adapter,
});

const app = express();
app.use("/admin", adapter.getRouter());

const server = app.listen(portReq, () => {
  const addr = server.address();
  const port = typeof addr === "object" && addr !== null ? addr.port : portReq;
  process.stdout.write(JSON.stringify({ event: "ready", port }) + "\n");
});

const shutdown = async () => {
  try {
    server.close();
    await queue.close();
  } finally {
    process.exit(0);
  }
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
