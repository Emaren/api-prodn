"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeCosmiframeWallet = void 0;
const extension_1 = require("./extension");
const makeCosmiframeWallet = (allowedParentOrigins) => new extension_1.CosmiframeWallet({
    ...extension_1.cosmiframeExtensionInfo,
    allowedParentOrigins,
});
exports.makeCosmiframeWallet = makeCosmiframeWallet;
