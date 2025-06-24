import { AssetList, Chain } from '@chain-registry/types';
import { ChainName, ChainRecord, Endpoints, SignerOptions } from '../types';
import { Logger } from './logger';
export declare function convertChain(chain: Chain | ChainName, assetLists: AssetList[], signerOptions?: SignerOptions, preferredEndpoints?: Endpoints, isLazy?: boolean, logger?: Logger): ChainRecord;
