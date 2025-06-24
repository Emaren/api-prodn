"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KeplrExtensionWallet = void 0;
const core_1 = require("@cosmos-kit/core");
const provider_extension_1 = require("@keplr-wallet/provider-extension");
const chain_wallet_1 = require("./chain-wallet");
const client_1 = require("./client");
class KeplrExtensionWallet extends core_1.MainWalletBase {
    constructor(walletInfo, preferredEndpoints) {
        super(walletInfo, chain_wallet_1.ChainKeplrExtension);
        this.preferredEndpoints = preferredEndpoints;
    }
    async initClient() {
        this.initingClient();
        try {
            const keplr = await provider_extension_1.Keplr.getKeplr();
            this.initClientDone(keplr ? new client_1.KeplrClient(keplr) : undefined);
        }
        catch (error) {
            this.initClientError(error);
        }
    }
}
exports.KeplrExtensionWallet = KeplrExtensionWallet;
