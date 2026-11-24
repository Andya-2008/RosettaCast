// firestore/functions/index.js
import { initializeApp } from "firebase-admin/app";

initializeApp();

export { onFragmentCreated }    from "./onFragmentCreated.js";
export { onBatchCreated }       from "./onBatchCreated.js";
export { onSentenceCreated }    from "./onSentenceCreated.js";
export { onSentenceUpdated }    from "./onSentenceUpdated.js";
export { onFragmentArrived } 	from "./onFragmentArrived.js";
export { onFlushStaleBuffers } from "./onFlushStaleBuffers.js";
export { usage }                from "./usage.js";