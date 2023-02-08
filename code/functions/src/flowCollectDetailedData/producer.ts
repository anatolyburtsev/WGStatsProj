import {getFirestoreDB, getFunctionUrl, initTaskQueue} from "../utils";
import {collection, getDocs, limit, orderBy, query, startAfter} from "firebase/firestore";
import {logger} from "firebase-functions/v2";
import {Buffer} from "node:buffer";

export const producerFlow2 = async (event: any) => {
  const queue = await initTaskQueue("consumerflow2");
  const targetURI = await getFunctionUrl("consumerflow2");

  const dateToRead = "2023-01-19";
  const db = await getFirestoreDB();

  const collectionName = `date/${dateToRead}/accounts/`;
  const batchSize = 256;

  // const date = new Date().toISOString().split("T")[0];
  const date = dateToRead;
  const collectionRef = collection(db, collectionName);
  let q = query(collectionRef, orderBy("accountId"), limit(batchSize));

  let totalMessageCount = 0;

  for (let i = 0; i < 50000; i++) {
    const batch = await getDocs(q);
    if (batch.empty) {
      break;
    }
    const batchData = batch.docs
      .map((doc) => doc.data());

    const messagePromises = batchData
      .filter((doc) => doc.accountInfo.statistics.all.battles > 1000)
      .map((doc) => doc.accountId)
      .map((accountId) => ({
        accountId,
        date}))
      .map((messageBody) => Buffer.from(JSON.stringify(messageBody)))
      .map((message) => queue.enqueue({message}, {uri: targetURI}));

    await Promise.all(messagePromises);

    totalMessageCount += messagePromises.length;
    logger.debug(`iteration: ${i}. Published ${messagePromises.length} messages in this batch. 
    Total: ${totalMessageCount}`);

    const lastId = batchData[batchData.length-1].accountId;
    q = query(collectionRef, orderBy("accountId"), limit(batchSize), startAfter(lastId));
  }
};
