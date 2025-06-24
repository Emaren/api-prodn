"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WalletRepo = void 0;
/* eslint-disable no-empty */
/* eslint-disable no-console */
const client_1 = require("@chain-registry/client");
const state_1 = require("./bases/state");
/**
 * Store all ChainWallets for a particular Chain.
 */
class WalletRepo extends state_1.StateBase {
    isActive = false;
    chainRecord;
    _wallets;
    namespace = 'cosmos';
    session;
    repelWallet = true;
    callbackOptions;
    fetchInfo = false;
    constructor(chainRecord, wallets = []) {
        super();
        this.chainRecord = chainRecord;
        this._wallets = wallets;
        if (this.repelWallet) {
            this.wallets.forEach((w) => {
                w.updateCallbacks({
                    ...w.callbacks,
                    beforeConnect: async () => {
                        this.wallets.forEach(async (w2) => {
                            if (!w2.isWalletDisconnected && w2 !== w) {
                                await w2.disconnect(false, this.callbackOptions?.beforeConnect?.disconnect);
                            }
                        });
                    },
                });
            });
        }
        if (!this.chainRecord.chain) {
            this.fetchInfo = true;
            const registry = new client_1.ChainRegistryFetcher({
                urls: [
                    `https://raw.githubusercontent.com/cosmos/chain-registry/master/${chainRecord.name}/chain.json`,
                ],
            });
            registry
                .fetchUrls()
                .then(() => {
                this.chainRecord.chain = registry.getChain(chainRecord.name);
                this.actions?.render?.((i) => i + 1);
            })
                .catch((e) => {
                this.chainRecord.chain = null;
                this.logger?.warn(`Failed to fetch chain info for chain ${chainRecord.name}: [${e.name}] ${e.message}`);
            });
        }
        if (!this.chainRecord.assetList) {
            this.fetchInfo = true;
            const registry = new client_1.ChainRegistryFetcher({
                urls: [
                    `https://raw.githubusercontent.com/cosmos/chain-registry/master/${chainRecord.name}/assetlist.json`,
                ],
            });
            registry
                .fetchUrls()
                .then(() => {
                this.chainRecord.assetList = registry.getChainAssetList(chainRecord.name);
                // this.actions?.render?.((i) => i + 1);
            })
                .catch((e) => {
                this.chainRecord.assetList = null;
                this.logger?.warn(`Failed to fetch assetList info for chain ${chainRecord.name}: [${e.name}] ${e.message}`);
            });
        }
        if (this.fetchInfo) {
            this._wallets.forEach((wallet) => (wallet.chainRecord = this.chainRecord));
        }
    }
    setCallbackOptions(options) {
        this.callbackOptions = options;
    }
    setEnv(env) {
        this._env = env;
        this.wallets.forEach((w) => w.setEnv(env));
    }
    activate() {
        this.isActive = true;
        this.wallets.forEach((w) => w.activate());
    }
    get chainName() {
        return this.chainRecord.name;
    }
    get chainLogo() {
        return (
        // until chain_registry fix this
        // this.chainInfo.chain.logo_URIs?.svg ||
        // this.chainInfo.chain.logo_URIs?.png ||
        // this.chainInfo.chain.logo_URIs?.jpeg ||
        this.chainRecord.assetList?.assets[0]?.logo_URIs?.svg ||
            this.chainRecord.assetList?.assets[0]?.logo_URIs?.png ||
            undefined);
    }
    get wallets() {
        return this._wallets;
    }
    get platformEnabledWallets() {
        return this.isMobile
            ? this._wallets.filter((w) => typeof w.walletInfo.mobileDisabled === 'boolean'
                ? !w.walletInfo.mobileDisabled
                : !w.walletInfo.mobileDisabled())
            : this._wallets;
    }
    get isSingleWallet() {
        return this.wallets.length === 1;
    }
    get current() {
        if (!this.repelWallet) {
            this.logger.warn('when `repelWallet` is set false, `current` is always undefined.');
            return void 0;
        }
        return this.wallets.find((w) => !w.isWalletNotExist && !w.isWalletDisconnected);
    }
    getWallet = (walletName) => {
        return this.wallets.find((w) => w.walletName === walletName);
    };
    openView = () => {
        this.actions?.viewWalletRepo?.(this);
        this.actions?.viewOpen?.(true);
    };
    closeView = () => {
        this.actions?.viewOpen?.(false);
    };
    connect = async (walletName, sync = true) => {
        if (walletName) {
            const wallet = this.getWallet(walletName);
            await wallet?.connect(sync);
        }
        else {
            this.openView();
        }
    };
    disconnect = async (walletName, sync = true, options) => {
        if (walletName) {
            await this.getWallet(walletName)?.disconnect(sync, options);
        }
        else {
            await this.current.disconnect(sync, options);
        }
    };
    getRpcEndpoint = async (isLazy) => {
        for (const wallet of this.wallets) {
            try {
                return await wallet.getRpcEndpoint(isLazy);
            }
            catch (error) {
                this.logger?.debug(`${error.name}: ${error.message}`);
            }
        }
        return Promise.reject(`No valid RPC endpoint for chain ${this.chainName}!`);
    };
    getRestEndpoint = async (isLazy) => {
        for (const wallet of this.wallets) {
            try {
                return await wallet.getRestEndpoint(isLazy);
            }
            catch (error) {
                this.logger?.debug(`${error.name}: ${error.message}`);
            }
        }
        return Promise.reject(`No valid REST endpoint for chain ${this.chainName}!`);
    };
    getStargateClient = async () => {
        for (const wallet of this.wallets) {
            try {
                return await wallet.getStargateClient();
            }
            catch (error) {
                this.logger?.debug(`${error.name}: ${error.message}`);
            }
        }
        return Promise.reject(`Something wrong! Probably no valid RPC endpoint for chain ${this.chainName}.`);
    };
    getCosmWasmClient = async () => {
        for (const wallet of this.wallets) {
            try {
                return await wallet.getCosmWasmClient();
            }
            catch (error) {
                this.logger?.debug(`${error.name}: ${error.message}`);
            }
        }
        return Promise.reject(`Something wrong! Probably no valid RPC endpoint for chain ${this.chainName}.`);
    };
    getNameService = async () => {
        for (const wallet of this.wallets) {
            try {
                return await wallet.getNameService();
            }
            catch (error) {
                this.logger?.debug(`${error.name}: ${error.message}`);
            }
        }
        return Promise.reject(`Something wrong! Probably no valid RPC endpoint or name service is not registered for chain ${this.chainName}.`);
    };
}
exports.WalletRepo = WalletRepo;
