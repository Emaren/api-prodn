"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.wallets = void 0;
const keplr_extension_1 = require("@cosmos-kit/keplr-extension");
const keplr_mobile_1 = require("@cosmos-kit/keplr-mobile");
exports.wallets = [...keplr_extension_1.wallets, ...keplr_mobile_1.wallets];
