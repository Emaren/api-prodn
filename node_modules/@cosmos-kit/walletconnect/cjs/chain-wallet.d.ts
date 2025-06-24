import { type ChainRecord, type Mutable, type Wallet, type WalletConnectOptions, ChainWalletBase } from '@cosmos-kit/core';
import { WCClient } from './client';
import { IWCClient } from './types';
export declare class ChainWC extends ChainWalletBase {
    WCClient: IWCClient;
    clientMutable: Mutable<WCClient>;
    options?: WalletConnectOptions;
    constructor(walletInfo: Wallet, chainInfo: ChainRecord, WCClient: IWCClient);
    setClientNotExist(): void;
}
