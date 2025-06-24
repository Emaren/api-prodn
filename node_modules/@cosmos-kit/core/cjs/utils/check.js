"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.checkKey = exports.checkInit = void 0;
function checkInit(target, targetName, msg) {
    if (target === undefined || target === null) {
        throw new Error(msg || `${targetName || 'Variable'} is not inited!`);
    }
}
exports.checkInit = checkInit;
function checkKey(target, key, targetName, msg) {
    if (!target.has(key)) {
        throw new Error(msg || `${key} not existed in Map ${targetName}!`);
    }
}
exports.checkKey = checkKey;
