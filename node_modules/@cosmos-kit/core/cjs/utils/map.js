"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.valuesApply = void 0;
function valuesApply(target, callbackfn) {
    return new Map(Array.from(target).map(([key, value]) => [key, callbackfn(value)]));
}
exports.valuesApply = valuesApply;
