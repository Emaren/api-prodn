#!/bin/bash

# Ports you want to check
declare -A nodes
nodes["wolodev"]=1317
nodes["wolo-prod"]=1327
nodes["wolo-staging"]=1337

for node in "${!nodes[@]}"; do
  port=${nodes[$node]}
  echo "==============================="
  echo " $node (port $port)"
  echo "-------------------------------"
  
  curl -s http://localhost:$port/cosmos/base/tendermint/v1beta1/node_info | jq '.default_node_info.network, .application_version.cosmos_sdk_version' 2>/dev/null
  curl -s http://localhost:$port/cosmos/base/tendermint/v1beta1/syncing | jq 2>/dev/null
  curl -s http://localhost:$port/cosmos/base/tendermint/v1beta1/blocks/latest | jq '.block.header.height' 2>/dev/null
  
  echo ""
done
