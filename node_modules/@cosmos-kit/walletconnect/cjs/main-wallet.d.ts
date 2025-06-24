import type { Mutable, Wallet, WalletConnectOptions } from '@cosmos-kit/core';
import { MainWalletBase } from '@cosmos-kit/core';
import type { WCClient } from './client';
import { IChainWC, IWCClient } from './types';
export declare class WCWallet extends MainWalletBase {
    WCClient: IWCClient;
    clientMutable: Mutable<WCClient>;
    constructor(walletInfo: Wallet, ChainWC: IChainWC, WCClient: IWCClient);
    initClient(options?: WalletConnectOptions): Promise<void>;
}
