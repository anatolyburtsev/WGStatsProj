import {getFirestoreDB} from "../utils";
import {logger} from "firebase-functions/v2";
import {collection, getDocs, limit, orderBy, query, startAfter} from "firebase/firestore";


type accountDataType = {
  accountId: number,
}

// Function reads firestore collection and store it to GCStorage bucket
export const exportFirestoreCollection = async (collectionName: string, bucketName: string) => {
  const db = await getFirestoreDB();
  const collectionRef = collection(db, collectionName);
  const batchSize = 256;

  let q = query(collectionRef, orderBy("accountId"), limit(batchSize));

  let countOfRecords = 0;

  for (let i = 0; i < 50000; i++) {
    logger.info(`iteration: ${i}`);
    const batchData = await getDocs(q);
    if (batchData.empty) {
      break;
    }

    const cleanedData = batchData.docs.map((doc) => (doc.data() as accountDataType).accountId);
    logger.info(cleanedData);

    const lastId = cleanedData[cleanedData.length - 1];
    q = query(collectionRef, orderBy("accountId"), limit(batchSize), startAfter(lastId));

    countOfRecords += cleanedData.length;
  }

  logger.info(`Number of records: ${countOfRecords}`);

  return {
    "recordsCount": countOfRecords,
  };
};
