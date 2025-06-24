"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WCWallet = void 0;
const core_1 = require("@cosmos-kit/core");
class WCWallet extends core_1.MainWalletBase {
    WCClient;
    clientMutable = { state: core_1.State.Init };
    constructor(walletInfo, ChainWC, WCClient) {
        if (!walletInfo.walletconnect) {
            throw new Error(`'walletconnect' info for wallet ${walletInfo.prettyName} is not provided in wallet registry.`);
        }
        super(walletInfo, ChainWC);
        this.WCClient = WCClient;
    }
    async initClient(options) {
        if (!options) {
            this.initClientError(new Error('`walletconnectOptions` is not provided.'));
            return;
        }
        if (!options.signClient.projectId) {
            this.initClientError(new Error('`projectId` is not provided in `walletconnectOptions`.'));
            return;
        }
        this.initingClient();
        try {
            const client = new this.WCClient(this.walletInfo);
            client.logger = this.logger;
            client.emitter = this.emitter;
            client.env = this.env;
            client.options = options;
            this.initClientDone(client);
        }
        catch (error) {
            this.initClientError(error);
        }
    }
}
exports.WCWallet = WCWallet;
