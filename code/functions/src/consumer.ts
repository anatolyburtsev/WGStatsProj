import {Buffer} from "node:buffer";
import {logger} from "firebase-functions/v2";
import {SecretManagerServiceClient} from "@google-cloud/secret-manager";
import axios from "axios";
import {STEP} from "./constants";

import {initializeApp} from "firebase/app";
import {
  getFirestore,
  addDoc,
  collection,
} from "firebase/firestore/lite";


const secretVersionId = "projects/202233908638/secrets/hellosecret/versions/2";
const secretClient = new SecretManagerServiceClient();

const firebaseConfig = {
  apiKey: "AIzaSyAKRjYd_oGwH8O6vfJyZGnwLC-EWA9Yies",
  authDomain: "wgstatsproj.firebaseapp.com",
  databaseURL: "https://wgstatsproj-default-rtdb.firebaseio.com",
  projectId: "wgstatsproj",
  storageBucket: "wgstatsproj.appspot.com",
  messagingSenderId: "202233908638",
  appId: "1:202233908638:web:6145e184cf6d596ff85a8f",
  measurementId: "G-75P2QYLWJD",
};


// Initialize Firebase
const app = initializeApp(firebaseConfig);
const firestore = getFirestore(app);

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
  logger.debug("Message received: ", messageBody);
  const jsonBody = JSON.parse(messageBody);
  const startId = parseInt(jsonBody.startId);

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
  // logger.info("response");
  // logger.info(response);
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
    return addDoc(collection(firestore, "accounts"), {
      accountId: parseInt(accountId),
      accountInfo: accountData,
    });
  });

  await Promise.all(storeDataPromises);

  logger.info(`startId: ${startId}, applicationId: ${applicationId}, 
  Processed ${validAccountsData.length} valid accounts`);
  // logger.info(validAccountsData);
};
