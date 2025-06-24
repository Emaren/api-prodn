"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CosmiframeWallet = void 0;
const cosmiframe_1 = require("@dao-dao/cosmiframe");
const bases_1 = require("../../bases");
const chain_wallet_1 = require("./chain-wallet");
const client_1 = require("./client");
class CosmiframeWallet extends bases_1.MainWalletBase {
    constructor(walletInfo) {
        super(walletInfo, chain_wallet_1.ChainWalletCosmiframe);
    }
    get walletInfo() {
        return this._walletInfo;
    }
    async initClient() {
        this.initingClient();
        const cosmiframe = new cosmiframe_1.Cosmiframe(this.walletInfo.allowedParentOrigins);
        const ready = await cosmiframe.isReady();
        if (!ready) {
            this.initClientError(new Error('Failed to detect Cosmiframe parent of allowed origin wrapping this app in an iframe.'));
            return;
        }
        // Get metadata from parent.
        const metadata = await cosmiframe.getMetadata();
        if (metadata?.name && typeof metadata.name === 'string') {
            this.walletInfo.prettyName = metadata.name;
        }
        if (metadata?.imageUrl && typeof metadata.imageUrl === 'string') {
            this.walletInfo.logo = metadata.imageUrl;
        }
        this.initClientDone(new client_1.CosmiframeClient(cosmiframe));
    }
}
exports.CosmiframeWallet = CosmiframeWallet;
