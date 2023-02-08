import {onMessagePublished} from "firebase-functions/v2/pubsub";
import {onSchedule} from "firebase-functions/v2/scheduler";
import {PUBSUB_TOPICS} from "./constants";
import {onTaskDispatched} from "firebase-functions/v2/tasks";
import {consumerFn} from "./flowCollectAliveAccount/consumer";
import {producerCloudTaskFn} from "./flowCollectAliveAccount/producerCloudTask";
import {producerPubSubFn} from "./flowCollectAliveAccount/producerPubSub";


// Producer pubsub
exports.producerpubsub = onSchedule({
  schedule: "0 0 1 * *",
  timeoutSeconds: 1800,
  memory: "2GiB",
}, producerPubSubFn);

// Producer cloud task
exports.producercloudtask = onSchedule({
  schedule: "0 0 2 * *",
  timeoutSeconds: 1800,
  memory: "2GiB",
}, producerCloudTaskFn);

// Consumer PubSub
exports.consumerflow1pubsub = onMessagePublished({
  topic: PUBSUB_TOPICS.FIND_ALIVE_USERS,
}, consumerFn);

// Consumer Cloud Tasks
exports.consumerflow1cloudtask = onTaskDispatched({
  retryConfig: {
    maxAttempts: 5,
    minBackoffSeconds: 60,
  },
  rateLimits: {
    maxConcurrentDispatches: 6,
  },
}, consumerFn);


// exports.producercloudtaskflow2 = onSchedule({
//   schedule: "0 0 3 * *",
//   timeoutSeconds: 1800,
//   memory: "2GiB",
// }, producerFlow2);
//
//
// exports.consumerflow2 = onTaskDispatched({
//   retryConfig: {
//     maxAttempts: 5,
//     minBackoffSeconds: 60,
//   },
//   rateLimits: {
//     maxConcurrentDispatches: 18,
//   },
// }, consumerFlow2);
//
// exports.countfunc = onRequest({}, (req, res) => {
//   const {collectionName} = req.body;
//   const size = getNumberOfElements(collectionName);
//   res.status(200).json({...size});
// });
