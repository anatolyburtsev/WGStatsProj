import {PubSub} from "@google-cloud/pubsub";
import {Buffer} from "node:buffer";
import {onMessagePublished} from "firebase-functions/v2/pubsub";
import {logger} from "firebase-functions/v2";
import {onSchedule} from "firebase-functions/v2/scheduler";
import {consumerFn} from "./consumer";
import {PUBSUB_TOPICS, STEP} from "./constants";
import {defineInt} from "firebase-functions/params";

const pubsubTopicClient = new PubSub().topic(PUBSUB_TOPICS.FIND_ALIVE_USERS);

// const getEnvVarOrThrow = (varName: string): string => {
//   const value = process.env[varName];
//   if (!value) {
//     throw new Error(`Missing env var ${varName}`);
//   }
//   return value;
// };

// scheduled function run monthly
exports.producer = onSchedule("0 0 1 * *", async (event: any) => {
  const startIds: number[] = [];
  const startAccountId = defineInt("START_ACCOUNT_ID").value();
  const endAccountId = defineInt("END_ACCOUNT_ID").value();
  for (let i = startAccountId; i < endAccountId; i += STEP) {
    startIds.push(i);
  }

  const messages = startIds
    .map((id) => ({startId: id}))
    .map((message) => Buffer.from(JSON.stringify(message)));
  const publishPromises = messages.map((message) => {
    logger.info(`Publishing message ${message}`);
    return pubsubTopicClient.publishMessage({
      data: message,
    });
  });
  await Promise.all(publishPromises);

  logger.info(`Successfully pushed ${publishPromises.length} messages`);
});


exports.consumer = onMessagePublished({
  topic: PUBSUB_TOPICS.FIND_ALIVE_USERS,
  // maxInstances: 3,
  // memory: "256MiB",
  // concurrency: 1,
  retry: true,
  // cpu: 1,
}, consumerFn);
