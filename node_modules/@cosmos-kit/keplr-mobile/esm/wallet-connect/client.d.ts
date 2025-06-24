import { StdSignature, StdSignDoc } from '@cosmjs/amino';
import { OfflineDirectSigner } from '@cosmjs/proto-signing';
import { DirectSignDoc, SignOptions, SignType, SimpleAccount, Wallet, WalletAccount } from '@cosmos-kit/core';
import { WCClient } from '@cosmos-kit/walletconnect';
import { Keplr } from '@keplr-wallet/types';
import SignClient from '@walletconnect/sign-client';
import type { EngineTypes } from '@walletconnect/types';
import { ProposalTypes } from '@walletconnect/types';
export declare class KeplrClient extends WCClient {
    keplr: Keplr | undefined;
    constructor(walletInfo: Wallet);
    initKeplrWCClient(signClient: SignClient, options: {
        sessionProperties?: ProposalTypes.SessionProperties;
    }): void;
    connect(chainIds: string | string[], options?: EngineTypes.ConnectParams): Promise<void>;
    enable(chainIds: string | string[]): Promise<void>;
    getSimpleAccount(chainId: string): Promise<SimpleAccount>;
    getAccount(chainId: string): Promise<WalletAccount>;
    getOfflineSigner(chainId: string, preferredSignType?: SignType): Promise<import("@keplr-wallet/types").OfflineAminoSigner | OfflineDirectSigner>;
    getOfflineSignerAmino(chainId: string): import("@keplr-wallet/types").OfflineAminoSigner;
    getOfflineSignerDirect(chainId: string): OfflineDirectSigner;
    signAmino(chainId: string, signer: string, signDoc: StdSignDoc, signOptions?: SignOptions): Promise<import("@keplr-wallet/types").AminoSignResponse>;
    signDirect(chainId: string, signer: string, signDoc: DirectSignDoc, signOptions?: SignOptions): Promise<import("@keplr-wallet/types").DirectSignResponse>;
    signArbitrary(chainId: string, signer: string, data: string | Uint8Array): Promise<StdSignature>;
}
