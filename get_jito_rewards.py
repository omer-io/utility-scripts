import csv
import time
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

    retries = 5
    for attempt in range(retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list) and len(data) > 0 and "bundle_id" in data[0]:
                        return data[0]["bundle_id"]
                    else:
                        return None
                elif response.status == 429 or response.status == 403:
                    print(f"HTTP ERROR {response.status} for signature {signature}. Attempt {attempt + 1}/{retries}")
                    if attempt < retries - 1:
                        await asyncio.sleep(0.5)
                    else:
                        print(f"Max retries reached for {signature}")
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
            print("txns requests completed: ", requests_done, " | tx not found: ", tx_not_found)

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

def get_block_rewards(block_data, slot):
    lamports = 0
    if "result" in block_data and "rewards" in block_data["result"]:
        rewards = block_data["result"]["rewards"]
        # Filter rewards for type "Fee"
        fee_rewards = [reward for reward in rewards if reward.get("rewardType") == "Fee"]

        if fee_rewards:
            for reward in fee_rewards:
                lamports = reward["lamports"]  # Extract lamports
                sol_value = lamports / 1_000_000_000  # Convert to SOL
                print(f"Slot: {slot} | Pubkey: {reward['pubkey']} | Lamports: {lamports} | SOL: {sol_value:.9f} | Type: {reward['rewardType']}")
        else:
            print("No 'Fee' rewards found in this block.")
    else:
        print("No rewards data found for this block.")

    return lamports

def get_vote_fee(transaction):
    log_messages = transaction.get("meta", {}).get("logMessages", [])
    vote_fee = 0
    if "Program Vote111111111111111111111111111111111111111 success" in log_messages:
        vote_fee = transaction.get("meta", {}).get("fee", 0)  

    return vote_fee

def get_block_data(slot):
    retries = 5
    for attempt in range(retries):
        try:
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
                return None, None, None, None, None

            transactions = block_data.get("result", {}).get("transactions", [])
            print(f"Txns in slot {slot}: {len(transactions)}")
            total_txns = len(transactions)

            non_vote_txns = [tx for tx in transactions if not is_vote_tx(tx)]
            print(f"Non-vote Txns in slot {slot}: {len(non_vote_txns)}")

            block_rewards = get_block_rewards(block_data, slot)
            vote_rewards = sum(get_vote_fee(tx) for tx in transactions)
            vote_rewards = vote_rewards/2
            nonvote_rewards = block_rewards - vote_rewards

            return total_txns, block_rewards, vote_rewards, nonvote_rewards, non_vote_txns
        
        except Exception as e:
            print(f"Attempt {attempt + 1} failed in getting block data: {e}")
            if attempt < retries - 1:
                time.sleep(1) 
            else:
                print("Max retries reached in getting block data. Returning None.")
                return None, None, None, None, None

def is_vote_tx(transaction):
    log_messages = transaction.get("meta", {}).get("logMessages", [])
    vote_tx = False
    if "Program Vote111111111111111111111111111111111111111 success" in log_messages:
        vote_tx = True
    
    return vote_tx  

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("first_slot", type=int, help="Enter first slot of your turn")
    
    args = parser.parse_args()
    first_slot = args.first_slot

    prev_turn_slots = [first_slot - i for i in range(4, 0, -1)]
    our_turn_slots = [first_slot + i for i in range(4)]
    next_turn_slots = [first_slot + i for i in range(4, 8)]
    turns = [prev_turn_slots, our_turn_slots, next_turn_slots]
    turn_labels = ["Previous Turn", "Our Turn", "Next Turn"]
    print("Previous Turn Slots:", prev_turn_slots)
    print("Our Turn Slots:", our_turn_slots)
    print("Next Turn Slots:", next_turn_slots)

    max_requests_per_second = 10
    global requests_done, rate_limit_error, tx_not_found

    results = []
    with open("jito_summary.csv", mode="w", newline="") as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(["Prev Slots", "Bundles", "Jito Txns", "Jito Rewards", "Block Rewards (NonVote)", "Total Txns (NonVote)", "Our Slots", "Bundles", "Jito Txns", "Jito Rewards", "Block Rewards (NonVote)", "Total Txns (NonVote)", "Next Slots", "Bundles", "Jito Txns", "Jito Rewards", "Block Rewards (NonVote)", "Total Txns (NonVote)"])
        # csv_writer.writerow(["Turn", "Slot(s)", "Bundles", "Jito Txns", "Jito Rewards", "Total Txns", "NonVote Txns", "Total Block Rewards", "NonVote Rewards", "Vote Rewards"])
        file.flush()
        for i, each_turn in enumerate(turns):
            print(f"\nExecuting: {turn_labels[i]} -> {each_turn}")
            total_jito_fee = 0
            total_jito_txns = 0
            total_bundles = 0
            total_block_rewards = 0
            total_vote_rewards = 0
            total_nonvote_rewards = 0
            total_txns = 0
            total_nonvote_txns = 0

            for slot in each_turn:
                requests_done = 0
                rate_limit_error = 0
                tx_not_found = 0
                slot_txns, block_rewards, vote_rewards, nonvote_rewards, non_vote_txns = get_block_data(slot)

                if slot_txns and non_vote_txns:
                    total_txns += slot_txns
                    total_nonvote_txns += len(non_vote_txns)
                    total_block_rewards += block_rewards
                    total_vote_rewards += vote_rewards
                    total_nonvote_rewards += nonvote_rewards

                    jito_txns, bundle_ids = asyncio.run(rate_limiter(non_vote_txns, max_requests_per_second))
                    num_of_signatures = sum(len(tx["transaction"]["signatures"]) for tx in jito_txns)
                    bundle_ids = list(set(bundle_ids))
                    print("txns requests completed: ", len(non_vote_txns), " | jito txns: ", len(jito_txns), " | bundles:", len(bundle_ids))
                    jito_fee = 0
                    for tx in jito_txns:
                        jito_fee += tx.get("meta", {}).get("fee", 0)

                    jito_fee = jito_fee - (num_of_signatures * 2500)
                    print(f"number of jito txns signatures for slot {slot}: {num_of_signatures}")
                    print(f"Jito fee for slot {slot}: {jito_fee}")
                    total_jito_fee += jito_fee
                    total_jito_txns += len(jito_txns)
                    total_bundles += len(bundle_ids)

                    # csv_writer.writerow([turn_labels[i], slot, len(bundle_ids), len(jito_txns), jito_fee, slot_txns, len(non_vote_txns), block_rewards, nonvote_rewards, vote_rewards])
                    print(f"{turn_labels[i]} | Slot {slot} | Bundles {len(bundle_ids)} | Jito Txns {len(jito_txns)} | Jito Rewards {jito_fee} | Block Rewards (NonVote) {int(nonvote_rewards)} | Total Txns (NonVote) {len(non_vote_txns)}")
                    # file.flush()
                else:
                    print(f"ERROR: NonVote txns not found for slot: {slot}, skipping...")

            # csv_writer.writerow([turn_labels[i], f"{each_turn[0]} - {each_turn[-1]}", total_bundles, total_jito_txns, total_jito_fee, total_txns, total_nonvote_txns, total_block_rewards, total_nonvote_rewards, total_vote_rewards])
            results.extend([f"{each_turn[0]} - {each_turn[-1]}", total_bundles, total_jito_txns, total_jito_fee, int(total_nonvote_rewards), total_nonvote_txns])
            # csv_writer.writerow([f"{each_turn[0]} - {each_turn[-1]}", total_bundles, total_jito_txns, total_jito_fee, int(total_nonvote_rewards), total_nonvote_txns])
            # file.flush()
            print(f"\nTurn {each_turn} | Bundles {total_bundles} | Jito Txns {total_jito_txns} | Jito Rewards {total_jito_fee} | Total Txns {total_txns}| Total NonVote Txns {total_nonvote_txns} | Total Block Rewards {total_block_rewards} | Total NonVote Rewards {int(total_nonvote_rewards)} | Total Vote Rewards {int(total_vote_rewards)}")

        csv_writer.writerow(results)
        file.flush()

if __name__ == "__main__":
    main()