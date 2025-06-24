import { nameServiceRegistries } from '../config';
export const getNameServiceRegistryFromChainName = (chainName) => {
    const registry = nameServiceRegistries.find((r) => r.chainName === chainName);
    if (!registry) {
        throw new Error('No such name service registered with chain ' + chainName);
    }
    return registry;
};
export const getNameServiceRegistryFromName = (name) => {
    return nameServiceRegistries.find((r) => r.name === name);
};
export const getNameServiceNameFromChainName = (chainName) => {
    return nameServiceRegistries.find((r) => r.chainName === chainName)?.name;
};
export const getChainNameFromNameServiceName = (name) => {
    return nameServiceRegistries.find((r) => r.name === name)?.chainName;
};
