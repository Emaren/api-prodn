import { MainWalletBase } from '../../bases';
import { CosmiframeWalletInfo } from './types';
export declare class CosmiframeWallet extends MainWalletBase {
    constructor(walletInfo: CosmiframeWalletInfo);
    get walletInfo(): CosmiframeWalletInfo;
    initClient(): Promise<void>;
}
