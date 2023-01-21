import {getCountFromServer, collection} from "firebase/firestore";

import {getFirestoreDB} from "./consumer";
import {logger} from "firebase-functions/v2";

/**
 * function gets http request with firestore collection name and collects number of elements in collection
 * returns number of elements in collection
 * @param collectionName
 */
export async function getNumberOfElements(collectionName: string) {
  const db = await getFirestoreDB();
  const colRef = collection(db, collectionName);
  const snapshot = await getCountFromServer(colRef);
  const size = snapshot.data().count;
  logger.info(`Collection: ${collectionName} has ${size} elements`);
  return {
    collectionName,
    size,
  };
}
