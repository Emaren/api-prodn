import { Cosmiframe } from '@dao-dao/cosmiframe';
import { MainWalletBase } from '../../bases';
import { ChainWalletCosmiframe } from './chain-wallet';
import { CosmiframeClient } from './client';
export class CosmiframeWallet extends MainWalletBase {
    constructor(walletInfo) {
        super(walletInfo, ChainWalletCosmiframe);
    }
    get walletInfo() {
        return this._walletInfo;
    }
    async initClient() {
        this.initingClient();
        const cosmiframe = new Cosmiframe(this.walletInfo.allowedParentOrigins);
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
        this.initClientDone(new CosmiframeClient(cosmiframe));
    }
}
