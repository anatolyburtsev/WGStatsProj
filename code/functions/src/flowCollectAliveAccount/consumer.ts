import {logger} from "firebase-functions/v2";
import {STEP} from "../constants";

import {
  setDoc,
  doc,
} from "firebase/firestore";
import {getApplicationId, getFirestoreDB, parseMessage, sendRequest} from "../utils";

type flow1Message = {
  startId: string,
  date: string
}

export const consumerFn = async (event: any) => {
  const db = await getFirestoreDB();
  const message = parseMessage<flow1Message>(event);
  const startId = parseInt(message.startId);
  const date = message.date;

  const accountIdList = [];
  for (let i = 0; i < STEP; i++) {
    accountIdList.push(startId + i);
  }
  const accountIds = accountIdList.join(",");

  const applicationId = await getApplicationId();
  logger.info(`startId: ${startId}, applicationId: ${applicationId}, Start`);
  const apiEndpoint = "https://api.wotblitz.com/wotb/account/info/";
  const requestUrl = `${apiEndpoint}?application_id=${applicationId}&account_id=${accountIds}`;
  const data = await sendRequest(requestUrl);

  const validAccountsData = Object.entries(data.data)
    .filter(([key, value]) => value !== null);

  const storeDataPromises = validAccountsData.map(([accountId, accountData]) => {
    const ref = doc(db, "date", date, "accounts", accountId);
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
