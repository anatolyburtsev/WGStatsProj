import {SecretManagerServiceClient} from "@google-cloud/secret-manager";
import {logger} from "firebase-functions/v2";
import {initializeApp} from "firebase/app";
import {initializeApp as initializeAppAdmin} from "firebase-admin/app";
import {applicationIdsSecretVersionId, firebaseConfigSecretVersionId} from "./constants";
import {GoogleAuth} from "google-auth-library";
import {getFunctions} from "firebase-admin/functions";
import {Buffer} from "node:buffer";
import axios from "axios";
import {getFirestore, Firestore} from "firebase/firestore";

const secretClient = new SecretManagerServiceClient();

export const getApplicationId = async (): Promise<string> => {
  const [version] = await secretClient.accessSecretVersion({name: applicationIdsSecretVersionId});
  const content = version?.payload?.data?.toString();
  if (content === undefined) {
    logger.error("Failed to get application id from secret manager");
    logger.error(version);
    throw new Error("Secret invalid format");
  }
  const keys: string[] = <string[]>JSON.parse(content);
  return keys[Math.floor(Math.random() * keys.length)];
};

export const getFirestoreDB = async (): Promise<Firestore> => {
  const [firebaseConfigVersion] = await secretClient.accessSecretVersion({name: firebaseConfigSecretVersionId});
  const firebaseConfig = JSON.parse(firebaseConfigVersion?.payload?.data?.toString() || "");
  const app = initializeApp(firebaseConfig, "consumer");
  return getFirestore(app);
};

export const parseMessage = <T>(event: any): T => {
  const message = event.data.message;
  const messageBody = Buffer.from(message.data, "base64").toString();
  logger.debug("Message received: ", messageBody);
  const jsonBody = JSON.parse(messageBody);
  return jsonBody as T;
};

/**
 * Get the URL of a given v2 cloud function.
 *
 * @param {string} name the function's name
 * @param {string} location the function's location
 * @return {Promise<string>} The URL of the function
 */
export async function getFunctionUrl(name: string, location = "us-central1") {
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

export const initTaskQueue = async (queueName: string) => {
  const secretClient = new SecretManagerServiceClient();
  const [firebaseConfigVersion] = await secretClient.accessSecretVersion({name: firebaseConfigSecretVersionId});
  const firebaseConfig = JSON.parse(firebaseConfigVersion?.payload?.data?.toString() || "");
  const app = initializeAppAdmin(firebaseConfig, "producerTask");
  return getFunctions(app).taskQueue(queueName);
};

export const sendRequest = async (url: string) => {
  const response = await axios.get(url);
  const data = response.data;
  if (data.status !== "ok") {
    if (data.status === "error") {
      logger.error(`Error in API request: ${data.error.message} to ${url}`);
      logger.error(data);
      throw new Error(data.error.message);
    } else {
      logger.error(`Unknown problem with API request to ${url}`);
      logger.error(data);
      throw new Error("Failed to get account info");
    }
  }
  return data;
};
