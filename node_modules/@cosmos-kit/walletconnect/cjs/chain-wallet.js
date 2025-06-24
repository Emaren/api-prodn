"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ChainWC = void 0;
const core_1 = require("@cosmos-kit/core");
class ChainWC extends core_1.ChainWalletBase {
    WCClient;
    clientMutable = { state: core_1.State.Init };
    options;
    constructor(walletInfo, chainInfo, WCClient) {
        super(walletInfo, chainInfo);
        this.WCClient = WCClient;
    }
    setClientNotExist() {
        this.setState(core_1.State.Error);
        this.setMessage(this.clientMutable.message);
    }
}
exports.ChainWC = ChainWC;
