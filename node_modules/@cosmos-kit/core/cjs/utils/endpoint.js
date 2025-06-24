"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getIsLazy = exports.isValidEndpoint = exports.getFastestEndpoint = void 0;
const axios_1 = __importDefault(require("axios"));
const _getValidEndpoint = async (endpoint, nodeType, logger) => {
    const valid = await (0, exports.isValidEndpoint)(endpoint, nodeType, false, logger);
    if (valid === false) {
        return Promise.reject('Invalid endpoint.');
    }
    else {
        return endpoint;
    }
};
const getFastestEndpoint = async (endpoints, nodeType, logger) => {
    try {
        // Ping ALL rpc providers and go with the one that resolves the fastest
        const fastestEndpoint = await Promise.any(endpoints.map((endpoint) => _getValidEndpoint(endpoint, nodeType, logger)));
        logger?.debug('rpcEndpoint won the race:', fastestEndpoint);
        return fastestEndpoint;
    }
    catch (e) {
        return Promise.reject(e);
    }
};
exports.getFastestEndpoint = getFastestEndpoint;
const isValidEndpoint = async (endpoint, nodeType, isLazy, logger) => {
    if (isLazy) {
        logger?.debug('Skipping test of accessibility for', endpoint);
        return true;
    }
    logger?.debug('Testing accessibility of', endpoint);
    try {
        let response;
        if (typeof endpoint === 'string') {
            response =
                nodeType === 'rest'
                    ? /**
                       * The verification process for the Rest API differs from that of RPC.
                       * Rather than checking for JSON-RPC availability, we verify the node
                       * information is available.
                       */
                        await axios_1.default.get(`${endpoint}/cosmos/base/tendermint/v1beta1/node_info`)
                    : /**
                       * SigningStargate uses JSON-RPC requests to broadcast transactions.
                       * To ensure that the node has this capability, conduct a POST request
                       * to the endpoint to verify its accessibility in advance. Note that
                       * SigningStargate also performs this internal check.
                       */
                        await axios_1.default.post(endpoint);
        }
        else {
            response =
                nodeType === 'rest'
                    ? /**
                       * The verification process for the Rest API differs from that of RPC.
                       * Rather than checking for JSON-RPC availability, we verify the node
                       * information is available.
                       */
                        await axios_1.default.get(`${endpoint.url}/cosmos/base/tendermint/v1beta1/node_info`, {
                            headers: endpoint.headers,
                        })
                    : /**
                       * SigningStargate uses JSON-RPC requests to broadcast transactions.
                       * To ensure that the node has this capability, conduct a POST request
                       * to the endpoint to verify its accessibility in advance. Note that
                       * SigningStargate also performs this internal check.
                       */
                        await axios_1.default.post(endpoint.url, {
                            headers: endpoint.headers,
                        });
        }
        if (response.status == 200) {
            logger?.debug('Access successfully.');
            return true;
        }
    }
    catch (err) {
        logger?.debug(`${err.name}: ${err.message}`);
    }
    logger?.debug('Access failed.');
    return false;
};
exports.isValidEndpoint = isValidEndpoint;
const getIsLazy = (globalIsLazy, chainIsLazy, endpointIsLazy, parameterIsLazy, logger) => {
    // logger?.debug(
    //   'Value of globalIsLazy, chainIsLazy, endpointIsLazy and parameterIsLazy:',
    //   globalIsLazy,
    //   chainIsLazy,
    //   endpointIsLazy,
    //   parameterIsLazy
    // );
    if (typeof parameterIsLazy !== 'undefined') {
        return parameterIsLazy;
    }
    if (typeof endpointIsLazy !== 'undefined') {
        return endpointIsLazy;
    }
    if (typeof chainIsLazy !== 'undefined') {
        return chainIsLazy;
    }
    if (typeof globalIsLazy !== 'undefined') {
        return globalIsLazy;
    }
    return false;
};
exports.getIsLazy = getIsLazy;
