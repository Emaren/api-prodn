"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.wallets = void 0;
const config_1 = require("./config");
const wallet_connect_1 = require("./wallet-connect");
const keplrMobile = new wallet_connect_1.KeplrMobileWallet(wallet_connect_1.keplrMobileInfo, config_1.preferredEndpoints);
exports.wallets = [keplrMobile];
