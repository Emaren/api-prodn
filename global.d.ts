// global.d.ts

import type firebase from "firebase/compat/app";

declare global {
  interface Window {
    firebase?: typeof firebase;
  }
}

export {};
