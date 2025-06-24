import type { CosmWasmClient } from '@cosmjs/cosmwasm-stargate';
import type { StargateClient } from '@cosmjs/stargate';
import type { ChainWalletBase } from './bases/chain-wallet';
import { StateBase } from './bases/state';
import type { NameService } from './name-service';
import { CallbackOptions, ChainRecord, DappEnv, DisconnectOptions, ExtendedHttpEndpoint, WalletName } from './types';
import type { Session } from './utils';
/**
 * Store all ChainWallets for a particular Chain.
 */
export declare class WalletRepo extends StateBase {
    isActive: boolean;
    chainRecord: ChainRecord;
    private _wallets;
    namespace: string;
    session: Session;
    repelWallet: boolean;
    private callbackOptions?;
    readonly fetchInfo: boolean;
    constructor(chainRecord: ChainRecord, wallets?: ChainWalletBase[]);
    setCallbackOptions(options?: CallbackOptions): void;
    setEnv(env?: DappEnv): void;
    activate(): void;
    get chainName(): string;
    get chainLogo(): string | undefined;
    get wallets(): ChainWalletBase[];
    get platformEnabledWallets(): ChainWalletBase[];
    get isSingleWallet(): boolean;
    get current(): ChainWalletBase | undefined;
    getWallet: (walletName: WalletName) => ChainWalletBase | undefined;
    openView: () => void;
    closeView: () => void;
    connect: (walletName?: WalletName, sync?: boolean) => Promise<void>;
    disconnect: (walletName?: WalletName, sync?: boolean, options?: DisconnectOptions) => Promise<void>;
    getRpcEndpoint: (isLazy?: boolean) => Promise<string | ExtendedHttpEndpoint>;
    getRestEndpoint: (isLazy?: boolean) => Promise<string | ExtendedHttpEndpoint>;
    getStargateClient: () => Promise<StargateClient>;
    getCosmWasmClient: () => Promise<CosmWasmClient>;
    getNameService: () => Promise<NameService>;
}
