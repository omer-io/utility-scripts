import argparse
import requests
import pandas as pd
import requests
import asyncio
from aiohttp import ClientSession

rpc_url = "https://mainnet.helius-rpc.com/?api-key=3ccd3ceb-7ef3-42e9-a155-708552f77a35"
requests_done = 0
rate_limit_error = 0
tx_not_found = 0

async def fetch_jito_tx(signature, session):
    global rate_limit_error
    url = f"https://bundles.jito.wtf/api/v1/bundles/transaction/{signature}"

    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                if isinstance(data, list) and len(data) > 0 and "bundle_id" in data[0]:
                    return data[0]["bundle_id"]
                else:
                    return None
            elif response.status == 429:
                print(f"HTTP ERROR (429) for signature {signature}")  
                return None         
            else:
                if response.status != 404:
                    print(f"HTTP error {response.status} for signature {signature}")
                return None
    except Exception as e:
        print(f"Error fetching data for signature {signature}: {e}")
        return None

# Asynchronous rate limiter
async def rate_limiter(txns, max_requests_per_second):
    global requests_done
    results = []
    bundle_ids = []
    num_of_requests = max_requests_per_second

    async def reset_requests():
        global tx_not_found
        global requests_done
        nonlocal num_of_requests
        while True:
            await asyncio.sleep(2.2)  # Reset every second
            num_of_requests = max_requests_per_second
            print("txns requests completed: ", requests_done, " | jito txns: ", requests_done - tx_not_found)

    async def worker(tx, session):
        global requests_done
        global tx_not_found
        nonlocal num_of_requests
        while num_of_requests <= 0:
            await asyncio.sleep(0.3)  # Wait until requests are available
        num_of_requests -= 1
        requests_done += 1
        bundle_id = await fetch_jito_tx(tx["transaction"]["signatures"][0], session)
        if bundle_id is not None:
            results.append(tx)
            bundle_ids.append(bundle_id)
        else:
            tx_not_found += 1

    async with ClientSession() as session:
        reset_task = asyncio.create_task(reset_requests())
        tasks = [worker(tx, session) for tx in txns]
        await asyncio.gather(*tasks)
        reset_task.cancel()

    return results, bundle_ids

def jito_tx(sig):
    url = f"https://bundles.jito.wtf/api/v1/bundles/transaction/{sig}"

    response = requests.get(url)
    print(response.json()) 

    if response.status_code == 200:
        data = response.json()
        return isinstance(data, list) and len(data) > 0 and "bundle_id" in data[0]

    return False 

def get_non_vote_txns(slot):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [slot, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
    }
    response = requests.post(rpc_url, json=payload)
    block_data = response.json()

    if "error" in block_data:
        print(f"Error fetching block {slot}: {block_data['error']['message']}")
        return None

    transactions = block_data.get("result", {}).get("transactions", [])
    print(f"Txns in slot {slot}: {len(transactions)}")

    non_vote_txns = [tx for tx in transactions if not is_vote_tx(tx)]
    print(f"Non-vote Txns in slot {slot}: {len(non_vote_txns)}")

    return non_vote_txns

def is_vote_tx(transaction):
    log_messages = transaction.get("meta", {}).get("logMessages", [])
    vote_tx = False
    if "Program Vote111111111111111111111111111111111111111 success" in log_messages:
        vote_tx = True
    
    return vote_tx  

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("slots", nargs="+", type=int, help="Enter one or more slot numbers")
    
    args = parser.parse_args()
    slots = args.slots
    max_requests_per_second = 20
    global requests_done, rate_limit_error, tx_not_found
    total_jito_fee = 0
    total_jito_txns = 0
    total_bundles = 0

    for slot in slots:
        requests_done = 0
        rate_limit_error = 0
        tx_not_found = 0
        non_vote_txns = get_non_vote_txns(slot)
        jito_txns, bundle_ids = asyncio.run(rate_limiter(non_vote_txns, max_requests_per_second))
        bundle_ids = list(set(bundle_ids))
        print("txns requests completed: ", len(non_vote_txns), " | jito txns: ", len(jito_txns), " | bundles:", len(bundle_ids))
        jito_fee = 0
        for tx in jito_txns:
            jito_fee += tx.get("meta", {}).get("fee", 0)

        print(f"Jito fee for slot {slot}: {jito_fee}")
        total_jito_fee += jito_fee
        total_jito_txns += len(jito_txns)
        total_bundles += len(bundle_ids)

    print(f"Slot(s) {slots} | Bundles {total_bundles} | Total Jito Txns {total_jito_txns} | Total Jito Rewards {total_jito_fee}")

if __name__ == "__main__":
    main()