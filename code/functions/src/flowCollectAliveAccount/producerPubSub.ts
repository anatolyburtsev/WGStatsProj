import {defineInt} from "firebase-functions/params";
import {CHUNK_SIZE, PUBSUB_TOPICS, STEP} from "../constants";
import {Buffer} from "node:buffer";
import {logger} from "firebase-functions/v2";
import {PubSub} from "@google-cloud/pubsub";

export const producerPubSubFn = async (event: any) => {
  logger.info(`request:  ${JSON.stringify(event)}`);
  const batchPublisher = new PubSub().topic(PUBSUB_TOPICS.FIND_ALIVE_USERS, {
    batching: {
      maxMessages: 10,
      maxMilliseconds: 10 * 1000,
    },
  });

  const startAccountId = defineInt("DEFAULT_START_ACCOUNT_ID").value();
  const endAccountId = defineInt("DEFAULT_END_ACCOUNT_ID").value();
  const date = new Date().toISOString().split("T")[0];
  const startIds: number[] = [];
  for (let chunkNumber = 0; chunkNumber <= (endAccountId - startAccountId) / STEP / CHUNK_SIZE; chunkNumber++) {
    startIds.length = 0;
    for (let i = 0; i < CHUNK_SIZE; i += 1) {
      startIds.push(startAccountId + i * STEP + chunkNumber * STEP * CHUNK_SIZE);
    }
    logger.debug(`pushing following ids: ${JSON.stringify(startIds)}`);
    const messagesPromises = startIds
      .map((id) => ({startId: id, date}))
      .map((messageBody) => Buffer.from(JSON.stringify(messageBody)))
      // publish message to PubSub
      .map((message) => batchPublisher.publishMessage({
        data: message,
      }));

    await Promise.all(messagesPromises);
    logger.debug(`Published chunk starting with i: ${chunkNumber}, chunk size: ${CHUNK_SIZE}`);
  }
};
