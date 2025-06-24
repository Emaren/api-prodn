#!/usr/bin/env python3

import sys
import requests

if len(sys.argv) != 2:
    print("Usage: address_inspector.py <bech32-address>")
    sys.exit(1)

address = sys.argv[1]
API_BASE = "https://api.aoe2hdbets.com"

def query(path):
    url = f"{API_BASE}{path}"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {url}")
        return None
    return resp.json()

print("Checking address:", address)

balance = query(f"/cosmos/bank/v1beta1/balances/{address}")
delegations = query(f"/cosmos/staking/v1beta1/delegations/{address}")
rewards = query(f"/cosmos/distribution/v1beta1/delegators/{address}/rewards")

print("\n--- BALANCES ---")
print(balance or "No balance data")

print("\n--- DELEGATIONS ---")
print(delegations or "No delegations")

print("\n--- REWARDS ---")
print(rewards or "No rewards")
