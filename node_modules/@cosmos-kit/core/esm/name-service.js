export class NameService {
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
