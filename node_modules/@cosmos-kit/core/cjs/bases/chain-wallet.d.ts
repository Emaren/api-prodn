import { SigningCosmWasmClientOptions } from '@cosmjs/cosmwasm-stargate';
import { EncodeObject, OfflineSigner } from '@cosmjs/proto-signing';
import { SigningStargateClientOptions, StargateClientOptions, StdFee } from '@cosmjs/stargate';
import type { TxRaw } from 'cosmjs-types/cosmos/tx/v1beta1/tx';
import { ChainRecord, CosmosClientType, Endpoints, ExtendedHttpEndpoint, SignType, SimpleAccount, Wallet } from '../types';
import type { MainWalletBase } from './main-wallet';
import { WalletBase } from './wallet';
export declare class ChainWalletBase extends WalletBase {
    mainWallet: MainWalletBase;
    chainRecord: ChainRecord;
    protected _rpcEndpoint?: string | ExtendedHttpEndpoint;
    protected _restEndpoint?: string | ExtendedHttpEndpoint;
    connectChains?: () => Promise<any>;
    offlineSigner?: OfflineSigner;
    namespace: string;
    preferredSignType: SignType;
    constructor(walletInfo: Wallet, chainRecord: ChainRecord);
    get chain(): import("@chain-registry/types").Chain;
    get assetList(): import("@chain-registry/types").AssetList;
    get isTestNet(): boolean;
    get preferredEndpoints(): Endpoints;
    get rpcEndpoints(): (string | ExtendedHttpEndpoint)[];
    get restEndpoints(): (string | ExtendedHttpEndpoint)[];
    /**
     * stands for real `chainIsLazy` considered both `globalIsLazy` and `chainIsLazy` settings
     */
    get isLazy(): boolean;
    addEndpoints(endpoints?: Endpoints): void;
    get chainName(): string;
    get chainLogoUrl(): string | undefined;
    get stargateOptions(): StargateClientOptions | undefined;
    get signingStargateOptions(): SigningStargateClientOptions | undefined;
    get signingCosmwasmOptions(): SigningCosmWasmClientOptions | undefined;
    get assets(): import("@chain-registry/types").Asset[];
    get chainId(): string;
    get cosmwasmEnabled(): boolean;
    get username(): string | undefined;
    get address(): string | undefined;
    setData(data: SimpleAccount | undefined): void;
    initClient(_options?: any): void | Promise<void>;
    update(options?: {
        connect: boolean;
    }): Promise<void>;
    getRpcEndpoint: (isLazy?: boolean) => Promise<string | ExtendedHttpEndpoint>;
    getRestEndpoint: (isLazy?: boolean) => Promise<string | ExtendedHttpEndpoint>;
    getStargateClient: () => Promise<import("@cosmjs/stargate").StargateClient>;
    getCosmWasmClient: () => Promise<import("@cosmjs/cosmwasm-stargate").CosmWasmClient>;
    getNameService: () => Promise<import("../name-service").NameService>;
    initOfflineSigner(preferredSignType?: SignType): Promise<void>;
    getSigningStargateClient: () => Promise<import("@cosmjs/stargate").SigningStargateClient>;
    getSigningCosmWasmClient: () => Promise<import("@cosmjs/cosmwasm-stargate").SigningCosmWasmClient>;
    protected getSigningClient: (type?: CosmosClientType) => Promise<import("@cosmjs/stargate").SigningStargateClient | import("@cosmjs/cosmwasm-stargate").SigningCosmWasmClient>;
    estimateFee: (messages: EncodeObject[], type?: CosmosClientType, memo?: string, multiplier?: number) => Promise<StdFee>;
    sign: (messages: EncodeObject[], fee?: StdFee | number, memo?: string, type?: CosmosClientType) => Promise<TxRaw>;
    broadcast: (signedMessages: TxRaw, type?: CosmosClientType) => Promise<import("@cosmjs/cosmwasm-stargate").DeliverTxResponse>;
    signAndBroadcast: (messages: EncodeObject[], fee?: StdFee | number, memo?: string, type?: CosmosClientType) => Promise<import("@cosmjs/cosmwasm-stargate").DeliverTxResponse>;
}
