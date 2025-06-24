"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KeplrClient = void 0;
const keplr_1 = require("@chain-registry/keplr");
const long_1 = __importDefault(require("long"));
const session_1 = require("./session");
class KeplrClient {
    client;
    _defaultSignOptions = {
        preferNoSetFee: false,
        preferNoSetMemo: true,
        disableBalanceCheck: true,
    };
    get defaultSignOptions() {
        return this._defaultSignOptions;
    }
    setDefaultSignOptions(options) {
        this._defaultSignOptions = options;
    }
    constructor(client) {
        this.client = client;
    }
    async enable(chainIds) {
        await this.client.enable(chainIds);
    }
    async suggestToken({ chainId, tokens, type }) {
        if (type === 'cw20') {
            for (const { contractAddress, viewingKey } of tokens) {
                await this.client.suggestToken(chainId, contractAddress, viewingKey);
            }
        }
    }
    async getSimpleAccount(chainId) {
        const { address, username } = await this.getAccount(chainId);
        return {
            namespace: 'cosmos',
            chainId,
            address,
            username,
        };
    }
    async getAccount(chainId) {
        const key = await this.client.getKey(chainId);
        return {
            username: key.name,
            address: key.bech32Address,
            algo: key.algo,
            pubkey: key.pubKey,
            isNanoLedger: key.isNanoLedger,
        };
    }
    getOfflineSigner(chainId, preferredSignType) {
        switch (preferredSignType) {
            case 'amino':
                return this.getOfflineSignerAmino(chainId);
            case 'direct':
                return this.getOfflineSignerDirect(chainId);
            default:
                return this.getOfflineSignerAmino(chainId);
        }
        // return this.client.getOfflineSignerAuto(chainId);
    }
    getOfflineSignerAmino(chainId) {
        return {
            getAccounts: async () => {
                return [await this.getAccount(chainId)];
            },
            signAmino: async (signerAddress, signDoc) => {
                return this.signAmino(chainId, signerAddress, signDoc, this.defaultSignOptions);
            },
        };
        // return this.client.getOfflineSignerOnlyAmino(chainId);
    }
    getOfflineSignerDirect(chainId) {
        return {
            getAccounts: async () => {
                return [await this.getAccount(chainId)];
            },
            signDirect: async (signerAddress, signDoc) => {
                const resp = await this.signDirect(chainId, signerAddress, signDoc, this.defaultSignOptions);
                return {
                    ...resp,
                    signed: {
                        ...resp.signed,
                        accountNumber: BigInt(resp.signed.accountNumber.toString()),
                    },
                };
            },
        };
        // return this.client.getOfflineSigner(chainId) as OfflineDirectSigner;
    }
    async addChain(chainInfo) {
        // TODO later allow walletInfo getter to be available here
        // make this more generic
        const chainsAlreadyAdded = session_1.ExpiringLocalStorage.getItems('cosmos-kit@2:keplr/supported-chain');
        if (chainsAlreadyAdded && chainsAlreadyAdded.length > 0) {
            if (chainsAlreadyAdded.includes(chainInfo.name)) {
                return;
            }
        }
        const suggestChain = (0, keplr_1.chainRegistryChainToKeplr)(chainInfo.chain, chainInfo.assetList ? [chainInfo.assetList] : []);
        if (chainInfo.preferredEndpoints?.rest?.[0]) {
            suggestChain.rest =
                chainInfo.preferredEndpoints?.rest?.[0];
        }
        if (chainInfo.preferredEndpoints?.rpc?.[0]) {
            suggestChain.rpc =
                chainInfo.preferredEndpoints?.rpc?.[0];
        }
        try {
            await this.client.experimentalSuggestChain(suggestChain);
            session_1.ExpiringLocalStorage.addItem('cosmos-kit@2:keplr/supported-chain', chainInfo.name, 1000 * 60);
        }
        catch (error) {
            console.log('Error while adding chain', error);
            throw error;
        }
    }
    async signAmino(chainId, signer, signDoc, signOptions) {
        return await this.client.signAmino(chainId, signer, signDoc, signOptions || this.defaultSignOptions);
    }
    async signArbitrary(chainId, signer, data) {
        return await this.client.signArbitrary(chainId, signer, data);
    }
    async signDirect(chainId, signer, signDoc, signOptions) {
        const resp = await this.client.signDirect(chainId, signer, {
            ...signDoc,
            accountNumber: long_1.default.fromString(signDoc.accountNumber.toString()),
        }, signOptions || this.defaultSignOptions);
        return {
            ...resp,
            signed: {
                ...resp.signed,
                accountNumber: BigInt(resp.signed.accountNumber.toString()),
            },
        };
    }
    async sendTx(chainId, tx, mode) {
        return await this.client.sendTx(chainId, tx, mode);
    }
}
exports.KeplrClient = KeplrClient;
