"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConnectError = exports.WalletNotProvidedError = exports.ExpiredError = exports.RejectedError = exports.ClientNotExistError = void 0;
// Wallet
exports.ClientNotExistError = new Error('Client Not Exist!');
exports.RejectedError = new Error('Request Rejected!');
// QRCode
exports.ExpiredError = new Error('Expired!');
class WalletNotProvidedError extends Error {
    walletName;
    constructor(walletName) {
        super(`Wallet ${walletName} is not provided.`);
        this.walletName = walletName;
    }
}
exports.WalletNotProvidedError = WalletNotProvidedError;
class ConnectError extends Error {
    name = 'ConnectError';
    constructor(message) {
        super(message);
    }
}
exports.ConnectError = ConnectError;
