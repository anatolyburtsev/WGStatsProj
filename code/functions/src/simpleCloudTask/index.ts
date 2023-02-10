import {getFunctionUrl, initTaskQueue, parseMessage} from "../utils";
import {Buffer} from "node:buffer";
import * as _ from "lodash";
import {logger} from "firebase-functions/v2";
import {onTaskDispatched} from "firebase-functions/v2/tasks";
import {onSchedule} from "firebase-functions/v2/scheduler";

// has to match with producer function name
const TASK_QUEUE_NAME = "simplecloudtask";

type messageType = {
  accountId: string
}

export const simpleProducerFn = async (event: any) => {
  const queue = await initTaskQueue(TASK_QUEUE_NAME);
  const targetURI = await getFunctionUrl(TASK_QUEUE_NAME);
  const messagesPromises = _.range(0, 60)
    .map((idx) => ({"accountId": idx}))
    .map((body) => Buffer.from(JSON.stringify(body)))
    .map((message) => queue.enqueue({message}, {uri: targetURI}));

  await Promise.all(messagesPromises);

  logger.info(`enqueued ${messagesPromises.length} tasks`);
};
export const simpleConsumerFn = async (event: any) => {
  const message = parseMessage<messageType>(event);
  logger.info(`Started processing message: ${JSON.stringify(message)}`);
  await delay(15000);
  logger.info(`Finished processing message: ${JSON.stringify(message)}`);
};

export const simpleCloudTask = onTaskDispatched({
  retryConfig: {
    maxAttempts: 5,
    minBackoffSeconds: 60,
  },
  rateLimits: {
    maxConcurrentDispatches: 4,
  },
}, simpleConsumerFn);

export const simpleProducer = onSchedule({
  schedule: "0 0 1 * *",
  timeoutSeconds: 1800,
  memory: "256MiB",
}, simpleProducerFn);

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
