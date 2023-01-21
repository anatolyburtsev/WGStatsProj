import {defineInt} from "firebase-functions/params";
import {CHUNK_SIZE, firebaseConfigSecretVersionId, STEP} from "./constants";
import {Buffer} from "node:buffer";
import {logger} from "firebase-functions/v2";
import {getFunctions} from "firebase-admin/functions";
import {GoogleAuth} from "google-auth-library";
import {initializeApp} from "firebase-admin/app";
import {SecretManagerServiceClient} from "@google-cloud/secret-manager";

const initQueue = async () => {
  const secretClient = new SecretManagerServiceClient();
  const [firebaseConfigVersion] = await secretClient.accessSecretVersion({name: firebaseConfigSecretVersionId});
  const firebaseConfig = JSON.parse(firebaseConfigVersion?.payload?.data?.toString() || "");
  const app = initializeApp(firebaseConfig, "producerTask");
  return getFunctions(app).taskQueue("consumer2cloudtask");
};

export const producerCloudTaskFn = async (event: any) => {
  const queue = await initQueue();
  const targetURI = await getFunctionUrl("consumer2cloudtask");

  const startAccountId = defineInt("START_ACCOUNT_ID").value();
  const endAccountId = defineInt("END_ACCOUNT_ID").value();
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
      .map((message) => queue.enqueue({message}, {uri: targetURI}));

    await Promise.all(messagesPromises);
    logger.debug(`Published chunk starting with i: ${chunkNumber}, chunk size: ${CHUNK_SIZE}`);
  }
};


/**
 * Get the URL of a given v2 cloud function.
 *
 * @param {string} name the function's name
 * @param {string} location the function's location
 * @return {Promise<string>} The URL of the function
 */
async function getFunctionUrl(name: string, location = "us-central1") {
  const auth = new GoogleAuth({
    scopes: "https://www.googleapis.com/auth/cloud-platform",
  });
  const projectId = await auth.getProjectId();
  const url = "https://cloudfunctions.googleapis.com/v2beta/" +
    `projects/${projectId}/locations/${location}/functions/${name}`;

  const client = await auth.getClient();
  const res = await client.request({url});
  // @ts-ignore
  const uri = res.data?.serviceConfig?.uri;
  logger.info(`URI: ${uri}`);
  if (!uri) {
    throw new Error(`Unable to retreive uri for function at ${url}`);
  }
  return uri;
}


