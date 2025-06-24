"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getChainNameFromNameServiceName = exports.getNameServiceNameFromChainName = exports.getNameServiceRegistryFromName = exports.getNameServiceRegistryFromChainName = void 0;
const config_1 = require("../config");
const getNameServiceRegistryFromChainName = (chainName) => {
    const registry = config_1.nameServiceRegistries.find((r) => r.chainName === chainName);
    if (!registry) {
        throw new Error('No such name service registered with chain ' + chainName);
    }
    return registry;
};
exports.getNameServiceRegistryFromChainName = getNameServiceRegistryFromChainName;
const getNameServiceRegistryFromName = (name) => {
    return config_1.nameServiceRegistries.find((r) => r.name === name);
};
exports.getNameServiceRegistryFromName = getNameServiceRegistryFromName;
const getNameServiceNameFromChainName = (chainName) => {
    return config_1.nameServiceRegistries.find((r) => r.chainName === chainName)?.name;
};
exports.getNameServiceNameFromChainName = getNameServiceNameFromChainName;
const getChainNameFromNameServiceName = (name) => {
    return config_1.nameServiceRegistries.find((r) => r.name === name)?.chainName;
};
exports.getChainNameFromNameServiceName = getChainNameFromNameServiceName;
