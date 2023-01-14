import {Buffer} from "node:buffer";
import {logger} from "firebase-functions/v2";
import {SecretManagerServiceClient} from "@google-cloud/secret-manager";
import axios from "axios";
import {STEP} from "./constants";

const secretVersionId = "projects/202233908638/secrets/hellosecret/versions/2";
const secretClient = new SecretManagerServiceClient();

const getApplicationId = async (): Promise<string> => {
  const [version] = await secretClient.accessSecretVersion({name: secretVersionId});
  const content = version?.payload?.data?.toString();
  if (content === undefined) {
    logger.error("Failed to get application id from secret manager");
    logger.error(version);
    throw new Error("Secret invalid format");
  }
  const keys: string[] = <string[]>JSON.parse(content);
  return keys[Math.floor(Math.random() * keys.length)];
};

export const consumerFn = async (event: any) => {
  const message = event.data.message;
  const messageBody = Buffer.from(message.data, "base64").toString();
  logger.info("Message received: ", messageBody);
  const jsonBody = JSON.parse(messageBody);
  const startId = parseInt(jsonBody.startId);
  logger.info("Hello from worker");
  logger.info(startId);

  const accountIdList = [];
  for (let i = 0; i < STEP; i++) {
    accountIdList.push(startId + i);
  }
  const accountIds = accountIdList.join(",");

  const applicationId = await getApplicationId();
  logger.info(`applicationId: ${applicationId}`);
  const apiEndpoint = "https://api.wotblitz.com/wotb/account/info/";
  const requestUrl = `${apiEndpoint}?application_id=${applicationId}&account_id=${accountIds}`;

  const response = await axios.get(requestUrl);
  logger.info("response");
  logger.info(response);
  const data = response.data;
  if (data.status !== "ok") {
    if (data.status === "error") {
      logger.error(`Error in API request: ${data.error.message}`);
      throw new Error(data.error.message);
    } else {
      logger.error(`Failed to get account info for ${startId}`);
      logger.error(data);
      throw new Error("Failed to get account info");
    }
  }

  const validAccountIds: string[] = Object.entries(data.data)
    .filter(([key, value]) => value !== null)
    .map(([key, value]) => key);

  logger.info(`Found ${validAccountIds.length} valid accounts`);
  logger.info(validAccountIds);
};
