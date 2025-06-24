"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.StateBase = void 0;
const types_1 = require("../types");
const utils_1 = require("../utils");
class StateBase {
    _mutable;
    actions;
    _env;
    logger;
    constructor() {
        this._mutable = { state: types_1.State.Init };
    }
    get env() {
        return this._env;
    }
    setEnv(env) {
        this._env = env;
    }
    setActions = (actions) => {
        this.actions = actions;
    };
    get isMobile() {
        return this.env?.device === 'mobile';
    }
    get mutable() {
        return this._mutable;
    }
    get state() {
        return this.mutable.state;
    }
    get isInit() {
        return this.state === 'Init';
    }
    get isDone() {
        return this.state === 'Done';
    }
    get isError() {
        return this.state === 'Error';
    }
    get isPending() {
        return this.state === 'Pending';
    }
    get data() {
        return this.mutable.data;
    }
    get message() {
        return this.mutable.message;
    }
    setState(state) {
        this.logger?.debug(`[State Change] ${this.state} (${this.walletStatus}) -> ${state} (${this.chainName}/${this.walletName})`);
        this._mutable.state = state;
        this.actions?.state?.(state);
    }
    setData(data) {
        this._mutable.data = data;
        this.actions?.data?.(data);
    }
    setMessage(message) {
        this.logger?.debug(`[Message Set] ${message} (${this.chainName}/${this.walletName})`);
        this._mutable.message = message;
        this.actions?.message?.(message);
    }
    reset() {
        this.setData(undefined);
        this.setMessage(undefined);
        this.setState(types_1.State.Init);
    }
    get walletStatus() {
        return (0, utils_1.getWalletStatusFromState)(this.state, this.message);
    }
    get isWalletOnceConnect() {
        return this.isWalletConnected || this.isWalletError;
    }
    get isWalletConnecting() {
        return this.walletStatus === 'Connecting';
    }
    get isWalletConnected() {
        return this.walletStatus === 'Connected';
    }
    get isWalletDisconnected() {
        return this.walletStatus === 'Disconnected';
    }
    get isWalletRejected() {
        return this.walletStatus === 'Rejected';
    }
    get isWalletNotExist() {
        return this.walletStatus === 'NotExist';
    }
    get isWalletError() {
        return this.walletStatus === 'Error';
    }
}
exports.StateBase = StateBase;
