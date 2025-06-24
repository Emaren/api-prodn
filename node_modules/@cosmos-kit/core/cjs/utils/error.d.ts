export declare const ClientNotExistError: Error;
export declare const RejectedError: Error;
export declare const ExpiredError: Error;
export declare class WalletNotProvidedError extends Error {
    readonly walletName: string;
    constructor(walletName: string);
}
export declare class ConnectError extends Error {
    readonly name = "ConnectError";
    constructor(message?: string);
}
