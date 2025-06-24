"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.sum = void 0;
function sum(iterable, callbackfn, initialValue) {
    return Array.from(iterable).reduce(callbackfn, initialValue);
}
exports.sum = sum;
