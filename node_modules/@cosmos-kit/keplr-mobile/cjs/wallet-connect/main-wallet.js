"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KeplrMobileWallet = void 0;
const keplr_extension_1 = require("@cosmos-kit/keplr-extension");
const walletconnect_1 = require("@cosmos-kit/walletconnect");
const provider_extension_1 = require("@keplr-wallet/provider-extension");
const chain_wallet_1 = require("./chain-wallet");
const client_1 = require("./client");
class KeplrMobileWallet extends walletconnect_1.WCWallet {
    constructor(walletInfo, preferredEndpoints) {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        super(walletInfo, chain_wallet_1.ChainKeplrMobile, client_1.KeplrClient);
        this.preferredEndpoints = preferredEndpoints;
    }
    async initClient(options) {
        try {
            const keplr = await provider_extension_1.Keplr.getKeplr();
            const userAgent = window.navigator.userAgent;
            if (keplr && userAgent.includes('KeplrWalletMobile')) {
                this.initClientDone(keplr ? new keplr_extension_1.KeplrClient(keplr) : undefined);
            }
            else {
                await super.initClient(options);
            }
        }
        catch (error) {
            if (error instanceof Error) {
                if (error.message === 'Client Not Exist!') {
                    await super.initClient(options);
                    return;
                }
                this.initClientError(error);
            }
        }
    }
}
exports.KeplrMobileWallet = KeplrMobileWallet;
