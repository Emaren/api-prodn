"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getWalletStatusFromState = void 0;
const types_1 = require("../types");
const error_1 = require("./error");
const getWalletStatusFromState = (state, message) => {
    switch (state) {
        case 'Pending':
            return types_1.WalletStatus.Connecting;
        case 'Done':
            return types_1.WalletStatus.Connected;
        case 'Error':
            switch (message) {
                case error_1.ClientNotExistError.message:
                    return types_1.WalletStatus.NotExist;
                case error_1.RejectedError.message:
                    return types_1.WalletStatus.Rejected;
                default:
                    return types_1.WalletStatus.Error;
            }
        case 'Init':
            return types_1.WalletStatus.Disconnected;
        default:
            return types_1.WalletStatus.Disconnected;
    }
};
exports.getWalletStatusFromState = getWalletStatusFromState;
