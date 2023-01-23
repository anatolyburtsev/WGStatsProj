import {getApplicationId, getFirestoreDB, parseMessage, sendRequest} from "../utils";
import {doc, setDoc} from "firebase/firestore";
import {logger} from "firebase-functions/v2";


type flow2Message = {
  accountId: string,
  date: string
}

export const consumerFlow2 = async (event: any) => {
  logger.info(`event: ${JSON.stringify(event)}`);
  const db = await getFirestoreDB();
  const message = parseMessage<flow2Message>(event);
  const accountId = message.accountId;
  const date = message.date;

  const applicationId = await getApplicationId();
  logger.debug(`applicationId: ${applicationId}`);
  const apiEndpoint = "https://api.wotblitz.com/wotb/tanks/stats/";
  const requestUrl = `${apiEndpoint}?application_id=${applicationId}&account_id=${accountId}`;

  const response = await sendRequest(requestUrl);
  const data = response.data[accountId];
  logger.debug(`Got response: ${JSON.stringify(data)}`)


  const docPath = `date/${date}/account/${accountId}`;
  logger.debug(`docPath: ${docPath}`);
  const docRef = doc(db, docPath);
  logger.debug("store doc to db")
  await setDoc(docRef, {
    tanksStats: data
  }, {merge: true});

  logger.debug(`Processed account: ${accountId}, date: ${date}`);
};
