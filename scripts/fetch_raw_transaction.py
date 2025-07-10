import sys
import requests
import json

if len(sys.argv) != 3:
    print("Usage: python fetch_raw_transaction.py <SIGNATURE> <OUTPUT_FILE>")
    sys.exit(1)

signature = sys.argv[1]
output_file = sys.argv[2]

# Можно заменить на свой RPC endpoint, если есть
RPC_URL = "https://api.mainnet-beta.solana.com"

payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTransaction",
    "params": [signature, {"encoding": "json", "maxSupportedTransactionVersion": 0}]
}

resp = requests.post(RPC_URL, json=payload, timeout=30)
resp.raise_for_status()
data = resp.json()

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"Raw transaction saved to {output_file}") 