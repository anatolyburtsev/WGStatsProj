import {Buffer} from "node:buffer";
import {logger} from "firebase-functions/v2";
import {SecretManagerServiceClient} from "@google-cloud/secret-manager";
import axios from "axios";
import {firebaseConfigSecretVersionId, STEP} from "./constants";

import {initializeApp} from "firebase/app";
import {
  getFirestore,
  setDoc,
  doc,
} from "firebase/firestore/lite";

const applicationIdsSecretVersionId = "projects/202233908638/secrets/hellosecret/versions/2";
const secretClient = new SecretManagerServiceClient();

const getApplicationId = async (): Promise<string> => {
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

export const consumerFn = async (event: any) => {
  const [firebaseConfigVersion] = await secretClient.accessSecretVersion({name: firebaseConfigSecretVersionId});
  const firebaseConfig = JSON.parse(firebaseConfigVersion?.payload?.data?.toString() || "")
  logger.debug(`firebase config: ${JSON.stringify(firebaseConfig)}`)
  const app = initializeApp(firebaseConfig, "consumer");
  const firestore = getFirestore(app);

  const message = event.data.message;
  const messageBody = Buffer.from(message.data, "base64").toString();
  logger.debug("Message received: ", messageBody);
  const jsonBody = JSON.parse(messageBody);
  const startId = parseInt(jsonBody.startId);
  const date = jsonBody.date;

  const accountIdList = [];
  for (let i = 0; i < STEP; i++) {
    accountIdList.push(startId + i);
  }
  const accountIds = accountIdList.join(",");

  const applicationId = await getApplicationId();
  logger.info(`startId: ${startId}, applicationId: ${applicationId}, Start`);
  const apiEndpoint = "https://api.wotblitz.com/wotb/account/info/";
  const requestUrl = `${apiEndpoint}?application_id=${applicationId}&account_id=${accountIds}`;

  const response = await axios.get(requestUrl);
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

  const validAccountsData = Object.entries(data.data)
    .filter(([key, value]) => value !== null);

  const storeDataPromises = validAccountsData.map(([accountId, accountData]) => {
    const ref = doc(firestore, "date", date, "accounts", accountId);
    return setDoc( ref, {
      accountId: parseInt(accountId),
      accountInfo: accountData,
    }
    );
  });
  await Promise.all(storeDataPromises);

  logger.info(`startId: ${startId}, applicationId: ${applicationId}, 
  Processed ${validAccountsData.length} valid accounts`);
};
