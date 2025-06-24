"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SuggestTokenTypes = exports.WalletStatus = void 0;
var WalletStatus;
(function (WalletStatus) {
    WalletStatus["Disconnected"] = "Disconnected";
    WalletStatus["Connecting"] = "Connecting";
    WalletStatus["Connected"] = "Connected";
    WalletStatus["NotExist"] = "NotExist";
    WalletStatus["Rejected"] = "Rejected";
    WalletStatus["Error"] = "Error";
})(WalletStatus = exports.WalletStatus || (exports.WalletStatus = {}));
exports.SuggestTokenTypes = {
    CW20: 'cw20',
    ERC20: 'erc20',
};
