import {PubSub} from "@google-cloud/pubsub";
import {Buffer} from "node:buffer";
import {onMessagePublished} from "firebase-functions/v2/pubsub";
import {logger} from "firebase-functions/v2";
import {onSchedule} from "firebase-functions/v2/scheduler";

const start = 10000;
const end = 10401;
const step = 100;

const PUBSUB_TOPIC_NAME = "first-topic";
const pubsubTopicClient = new PubSub().topic(PUBSUB_TOPIC_NAME);

// scheduled function run monthly
exports.producer = onSchedule("0 0 1 * *", async (event: any) => {
  const startIds: number[] = [];
  for (let i = start; i < end; i += step) {
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

exports.worker = onMessagePublished(PUBSUB_TOPIC_NAME, (event: any) => {
  const message = event.data.message;
  const messageBody = Buffer.from(message.data, "base64").toString();
  const jsonBody = JSON.parse(messageBody);
  const startId = jsonBody.startId;
  logger.info("Hello from worker");
  logger.info(jsonBody);
  logger.info(startId);
});
