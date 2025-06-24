"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.NameService = void 0;
class NameService {
    client;
    registry;
    constructor(client, registry) {
        this.client = client;
        this.registry = registry;
    }
    async resolveName(address) {
        try {
            const { contract, getQueryMsg } = this.registry;
            const result = await this.client.queryContractSmart(contract, getQueryMsg(address));
            return result;
        }
        catch (e) {
            console?.error(e);
            return undefined;
        }
    }
}
exports.NameService = NameService;
