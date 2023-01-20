import {onMessagePublished} from "firebase-functions/v2/pubsub";
import {onSchedule} from "firebase-functions/v2/scheduler";
import {consumerFn} from "./consumer";
import {PUBSUB_TOPICS} from "./constants";
import {onTaskDispatched} from "firebase-functions/v2/tasks";
import {producerPubSubFn} from "./producerPubSub";
import {producerCloudTaskFn} from "./producerCloudTask";


// scheduled function run monthly
exports.producerpubsub = onSchedule({
  schedule: "0 0 1 * *",
  timeoutSeconds: 1800,
  memory: "2GiB",
}, producerPubSubFn);

exports.producercloudtask = onSchedule({
  schedule: "0 0 2 * *",
  timeoutSeconds: 1800,
  memory: "2GiB",
}, producerCloudTaskFn);


exports.consumer2cloudtask = onTaskDispatched({
  retryConfig: {
    maxAttempts: 5,
    minBackoffSeconds: 60,
  },
  rateLimits: {
    maxConcurrentDispatches: 6,
  },
}, consumerFn);

exports.consumer = onMessagePublished({
  topic: PUBSUB_TOPICS.FIND_ALIVE_USERS,
}, consumerFn);
