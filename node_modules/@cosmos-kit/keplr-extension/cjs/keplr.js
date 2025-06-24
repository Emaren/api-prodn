"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.wallets = void 0;
const config_1 = require("./config");
const extension_1 = require("./extension");
const keplrExtension = new extension_1.KeplrExtensionWallet(extension_1.keplrExtensionInfo, config_1.preferredEndpoints);
exports.wallets = [keplrExtension];
