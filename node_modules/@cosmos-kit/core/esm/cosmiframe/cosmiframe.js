import { cosmiframeExtensionInfo, CosmiframeWallet } from './extension';
export const makeCosmiframeWallet = (allowedParentOrigins) => new CosmiframeWallet({
    ...cosmiframeExtensionInfo,
    allowedParentOrigins,
});
