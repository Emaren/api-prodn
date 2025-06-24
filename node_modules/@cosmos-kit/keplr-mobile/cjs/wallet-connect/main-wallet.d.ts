import { EndpointOptions, Wallet, WalletConnectOptions } from '@cosmos-kit/core';
import { WCWallet } from '@cosmos-kit/walletconnect';
export declare class KeplrMobileWallet extends WCWallet {
    constructor(walletInfo: Wallet, preferredEndpoints?: EndpointOptions['endpoints']);
    initClient(options?: WalletConnectOptions): Promise<void>;
}
