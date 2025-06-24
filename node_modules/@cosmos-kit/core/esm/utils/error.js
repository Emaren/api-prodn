// Wallet
export const ClientNotExistError = new Error('Client Not Exist!');
export const RejectedError = new Error('Request Rejected!');
// QRCode
export const ExpiredError = new Error('Expired!');
export class WalletNotProvidedError extends Error {
    walletName;
    constructor(walletName) {
        super(`Wallet ${walletName} is not provided.`);
        this.walletName = walletName;
    }
}
export class ConnectError extends Error {
    name = 'ConnectError';
    constructor(message) {
        super(message);
    }
}
