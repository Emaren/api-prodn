"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.convertChain = void 0;
const endpoint_1 = require("./endpoint");
function convertChain(chain, assetLists, signerOptions, preferredEndpoints, isLazy, logger) {
    const chainName = typeof chain === 'string' ? chain : chain.chain_name;
    const assetList = assetLists.find((list) => list.chain_name === chainName);
    const _preferredEndpoints = {
        ...preferredEndpoints,
        isLazy: (0, endpoint_1.getIsLazy)(isLazy, preferredEndpoints?.isLazy, void 0, void 0, logger),
    };
    const converted = {
        name: chainName,
        chain: typeof chain === 'string' ? void 0 : chain,
        assetList,
        clientOptions: {
            stargate: signerOptions?.stargate?.(chain),
            signingStargate: signerOptions?.signingStargate?.(chain),
            signingCosmwasm: signerOptions?.signingCosmwasm?.(chain),
            preferredSignType: signerOptions?.preferredSignType?.(chain) || 'amino',
        },
        preferredEndpoints: _preferredEndpoints,
    };
    return converted;
}
exports.convertChain = convertChain;
