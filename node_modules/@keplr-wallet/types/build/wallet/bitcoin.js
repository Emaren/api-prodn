"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CHAIN_TYPE_TO_GENESIS_HASH = exports.GENESIS_HASH_TO_CHAIN_TYPE = exports.NETWORK_TO_GENESIS_HASH = exports.GENESIS_HASH_TO_NETWORK = exports.ChainType = exports.Network = exports.GenesisHash = exports.BitcoinSignMessageType = void 0;
var BitcoinSignMessageType;
(function (BitcoinSignMessageType) {
    BitcoinSignMessageType["ECDSA"] = "ecdsa";
    BitcoinSignMessageType["BIP322_SIMPLE"] = "bip322-simple";
})(BitcoinSignMessageType = exports.BitcoinSignMessageType || (exports.BitcoinSignMessageType = {}));
var GenesisHash;
(function (GenesisHash) {
    GenesisHash["MAINNET"] = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";
    GenesisHash["TESTNET"] = "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943";
    GenesisHash["SIGNET"] = "00000008819873e925422c1ff0f99f7cc9bbb232af63a077a480a3633bee1ef6";
})(GenesisHash = exports.GenesisHash || (exports.GenesisHash = {}));
var Network;
(function (Network) {
    Network["MAINNET"] = "mainnet";
    Network["LIVENET"] = "livenet";
    Network["TESTNET"] = "testnet";
    Network["SIGNET"] = "signet";
})(Network = exports.Network || (exports.Network = {}));
var ChainType;
(function (ChainType) {
    ChainType["BITCOIN_MAINNET"] = "BITCOIN_MAINNET";
    ChainType["BITCOIN_TESTNET"] = "BITCOIN_TESTNET";
    ChainType["BITCOIN_SIGNET"] = "BITCOIN_SIGNET";
})(ChainType = exports.ChainType || (exports.ChainType = {}));
exports.GENESIS_HASH_TO_NETWORK = {
    [GenesisHash.MAINNET]: Network.MAINNET,
    [GenesisHash.TESTNET]: Network.TESTNET,
    [GenesisHash.SIGNET]: Network.SIGNET,
};
exports.NETWORK_TO_GENESIS_HASH = {
    [Network.MAINNET]: GenesisHash.MAINNET,
    [Network.LIVENET]: GenesisHash.MAINNET,
    [Network.TESTNET]: GenesisHash.TESTNET,
    [Network.SIGNET]: GenesisHash.SIGNET,
};
exports.GENESIS_HASH_TO_CHAIN_TYPE = {
    [GenesisHash.MAINNET]: ChainType.BITCOIN_MAINNET,
    [GenesisHash.TESTNET]: ChainType.BITCOIN_TESTNET,
    [GenesisHash.SIGNET]: ChainType.BITCOIN_SIGNET,
};
exports.CHAIN_TYPE_TO_GENESIS_HASH = {
    [ChainType.BITCOIN_MAINNET]: GenesisHash.MAINNET,
    [ChainType.BITCOIN_TESTNET]: GenesisHash.TESTNET,
    [ChainType.BITCOIN_SIGNET]: GenesisHash.SIGNET,
};
//# sourceMappingURL=bitcoin.js.map