import {defineInt} from "firebase-functions/params";
import {CHUNK_SIZE, CLOUD_TASK_QUEUES, STEP} from "../constants";
import {Buffer} from "node:buffer";
import {logger} from "firebase-functions/v2";
import {getFunctionUrl, initTaskQueue} from "../utils";


export const producerCloudTaskFn = async (event: any) => {
  const queue = await initTaskQueue(CLOUD_TASK_QUEUES.FIND_ALIVE_USERS);
  const targetURI = await getFunctionUrl(CLOUD_TASK_QUEUES.FIND_ALIVE_USERS);

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
      // enqueue task to CloudTaskQueue
      .map((message) => queue.enqueue({message}, {uri: targetURI}));

    await Promise.all(messagesPromises);
    logger.debug(`Published chunk starting with i: ${chunkNumber}, chunk size: ${CHUNK_SIZE}`);
  }
};
