"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WalletBase = void 0;
const constants_1 = require("../cosmiframe/constants");
const types_1 = require("../types");
const utils_1 = require("../utils");
const state_1 = require("./state");
class WalletBase extends state_1.StateBase {
    clientMutable = { state: types_1.State.Init };
    emitter;
    _walletInfo;
    callbacks;
    session;
    walletConnectOptions;
    /**
     * isActive in mainWallet is not like chainWallet
     * - mainWallet: activated when connected
     * - chainWallet: activated when called by hooks (useChain, useChainWallet etc)
     */
    isActive = false;
    throwErrors = false;
    constructor(walletInfo) {
        super();
        this._walletInfo = walletInfo;
    }
    get appUrl() {
        return this.client?.appUrl;
    }
    get qrUrl() {
        return this.client?.qrUrl;
    }
    activate() {
        this.isActive = true;
    }
    inactivate() {
        this.isActive = false;
    }
    get client() {
        return this.clientMutable?.data;
    }
    initingClient() {
        this.clientMutable.state = types_1.State.Pending;
        this.clientMutable.message = void 0;
        this.clientMutable.data = void 0;
    }
    initClientDone(client) {
        this.clientMutable.data = client;
        this.clientMutable.state = types_1.State.Done;
        this.clientMutable.message = void 0;
    }
    initClientError(error) {
        this.logger?.error(`${this.walletPrettyName} initClientError: ${error?.message}`);
        this.clientMutable.message = error?.message;
        this.clientMutable.state = types_1.State.Error;
        if (this.isModeExtension) {
            this.setClientNotExist();
        }
        else {
            this.setError(`InitClientError: ${error.message}`);
        }
    }
    get walletInfo() {
        return this._walletInfo;
    }
    get isModeExtension() {
        return this.walletInfo.mode === 'extension';
    }
    get isModeWalletConnect() {
        return this.walletInfo.mode === 'wallet-connect';
    }
    get downloadInfo() {
        let downloads = this.walletInfo.downloads || [];
        downloads = downloads.filter((d) => d.device === this.env?.device || !d.device);
        if (downloads.length === 1) {
            return downloads[0];
        }
        downloads = downloads.filter((d) => d.os === this.env?.os || !d.os);
        if (downloads.length === 1) {
            return downloads[0];
        }
        downloads = downloads.filter((d) => d.browser === this.env?.browser || !d.browser);
        return downloads[0];
    }
    get walletName() {
        return this.walletInfo.name;
    }
    get walletPrettyName() {
        return this.walletInfo.prettyName;
    }
    get rejectMessageSource() {
        if (typeof this.walletInfo.rejectMessage === 'string') {
            return this.walletInfo.rejectMessage;
        }
        else {
            return this.walletInfo.rejectMessage?.source;
        }
    }
    get rejectMessageTarget() {
        if (typeof this.walletInfo.rejectMessage === 'string') {
            return void 0;
        }
        else {
            return this.walletInfo.rejectMessage?.target;
        }
    }
    get rejectCode() {
        return this.walletInfo.rejectCode;
    }
    rejectMatched(e) {
        return ((this.rejectMessageSource && e.message === this.rejectMessageSource) ||
            (this.rejectCode && e.code === this.rejectCode));
    }
    updateCallbacks(callbacks) {
        this.callbacks = { ...this.callbacks, ...callbacks };
    }
    _disconnect = async (sync, options) => {
        await this.callbacks?.beforeDisconnect?.();
        await this.client?.disconnect?.(options);
        if (this.clientMutable.state !== types_1.State.Error) {
            this.reset();
        }
        if (this.walletName !== constants_1.COSMIFRAME_WALLET_ID) {
            window.localStorage.removeItem('cosmos-kit@2:core//current-wallet');
        }
        if (sync) {
            this.emitter?.emit('sync_disconnect', this.chainName);
            this.logger?.debug('[WALLET EVENT] Emit `sync_disconnect`');
        }
        await this.callbacks?.afterDisconnect?.();
    };
    disconnect = async (sync, options) => {
        await this._disconnect(sync, options);
    };
    setClientNotExist(e) {
        const error = typeof e === 'undefined' ? new Error() : e;
        error.message = utils_1.ClientNotExistError.message;
        this.setError(error);
    }
    setRejected(e) {
        const error = typeof e === 'undefined' ? new Error() : e;
        error.message = utils_1.RejectedError.message;
        this.setError(error);
    }
    setError(e) {
        const error = typeof e === 'string' || typeof e === 'undefined' ? new Error(e) : e;
        this.setState(types_1.State.Error);
        this.setMessage(error.message);
        if (this.throwErrors === true) {
            throw error;
        }
        if (this.throwErrors === 'connect_only' && error.name === 'ConnectError') {
            throw error;
        }
    }
    connect = async (sync) => {
        await this.callbacks?.beforeConnect?.();
        const mobileDisabled = typeof this.walletInfo.mobileDisabled === 'boolean'
            ? this.walletInfo.mobileDisabled
            : this.walletInfo.mobileDisabled();
        if (this.isMobile && mobileDisabled) {
            this.setError(new utils_1.ConnectError('This wallet is not supported on mobile, please use desktop browsers.'));
            return;
        }
        if (sync) {
            this.emitter?.emit('sync_connect', this.chainName);
            this.logger?.debug(`[Event Emit] \`sync_connect\` (${this.chainName}/${this.walletName})`);
        }
        try {
            if (!this.client) {
                this.setState(types_1.State.Pending);
                this.setMessage('InitClient');
                await this.initClient(this.walletInfo.mode === 'wallet-connect'
                    ? this.walletConnectOptions
                    : void 0);
                this.emitter?.emit('broadcast_client', this.client);
                this.logger?.debug('[WALLET EVENT] Emit `broadcast_client`');
                if (!this.client) {
                    this.setClientNotExist(new utils_1.ConnectError());
                    return;
                }
            }
            await this.update();
        }
        catch (error) {
            this.setError(new utils_1.ConnectError(error.message));
        }
        await this.callbacks?.afterConnect?.();
    };
}
exports.WalletBase = WalletBase;
