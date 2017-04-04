#!/usr/bin/python3
import sys
import asyncio
import json
import datetime
import logging
import multiprocessing as mp

import aiohttp
from elasticsearch import Elasticsearch
from elasticsearch import helpers

logging.basicConfig(filename='error_blocks.log', level=logging.ERROR)

TX_INDEX_NAME = "ethereum-transaction"
B_INDEX_NAME = "ethereum-block"
HTTP_HEADERS = {"content-type": "application/json"}

# Elasticsearch maximum number of connections
ES_MAXSIZE = 10
# Elasticsearch host
ES_HOST = "localhost"
# Ethereum RPC endpoint
ETH_ENDPOINT = "http://localhost:8545"
# Parallel processing semaphore size
SEM_SIZE = 256
# Size of chunk size in blocks
CHUNK_SIZE = 500
# Size of multiprocessing Pool processing the chunks
POOL_SIZE = mp.cpu_count() + 2


def chunks(lst, nb_chunks):
    for i in range(0, len(lst), nb_chunks):
        yield lst[i:i + nb_chunks]


def make_request(block):
    return json.dumps({
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(block), True],
        "id": 1
    })


async def fetch(url, session, block, process_fn, actions):
    try:
        async with session.post(url, data=make_request(block), headers=HTTP_HEADERS) as response:
            process_fn(await response.json(), actions)
    except (aiohttp.ClientError, asyncio.TimeoutError) as exception:
        logging.error("block: " + str(block))
        print("Issue with block {}:\n{}\n".format(block, exception))


async def sema_fetch(sem, url, session, block, process_fn, actions):
    async with sem:
        await fetch(url, session, block, process_fn, actions)


async def run(block_range, process_fn, actions):
    tasks = []
    sem = asyncio.Semaphore(SEM_SIZE)

    # Create client session that will ensure we dont open new connection
    # per each request.
    async with aiohttp.ClientSession() as session:
        for i in block_range:
            # pass Semaphore and session to every POST request
            task = asyncio.ensure_future(sema_fetch(sem, ETH_ENDPOINT, session, i, process_fn, actions))
            tasks.append(task)

        await asyncio.gather(*tasks)


def process_block(block, actions):
    block = block["result"]

    transactions = block["transactions"]
    tx_hashes = list()
    tx_value_sum = 0

    block_nb = int(block["number"], 0)
    block_timestamp = datetime.datetime.fromtimestamp(int(block["timestamp"], 0))

    for tx in transactions:
        tx["blockNumber"] = int(tx["blockNumber"], 0)
        tx["blockTimestamp"] = block_timestamp
        # Convert wei into ether
        tx["value"] = int(tx["value"], 0) / 1000000000000000000.0
        tx_value_sum += tx["value"]
        actions.append(
            {"_index": TX_INDEX_NAME, "_type": "tx", "_id": tx["hash"], "_source": tx}
        )
        tx_hashes.append(tx["hash"])

    block["transactions"] = tx_hashes
    block["number"] = block_nb
    block["timestamp"] = block_timestamp
    block["gasLimit"] = int(block["gasLimit"], 0)
    block["gasUsed"] = int(block["gasUsed"], 0)
    block["size"] = int(block["size"], 0)
    block["transactionCount"] = len(tx_hashes)
    block["txValueSum"] = tx_value_sum

    actions.append({"_index": B_INDEX_NAME, "_type": "b", "_id": block_nb, "_source": block})


def setup_process(block_range):
    out_actions = list()

    elasticsearch = Elasticsearch([ES_HOST], maxsize=ES_MAXSIZE)

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(block_range, process_block, out_actions))
    loop.run_until_complete(future)

    blocks = [act for act in out_actions if act["_type"] == "b"]
    txs = [act for act in out_actions if act["_type"] == "tx"]

    if blocks or txs:
        try:
            helpers.bulk(elasticsearch, out_actions)
            print("#{}: ({}b, {}tx)".format(
                max([int(b["_id"]) for b in blocks]), len(blocks), len(txs)
            ))
        except helpers.BulkIndexError as exception:
            print("Issue with {} blocks:\n{}\n".format(len(blocks), exception))
            for act in blocks:
                logging.error("block: " + str(act["_id"]))


if __name__ == "__main__":

    if len(sys.argv) == 2:
        with open(sys.argv[1]) as f:
            CONTENT = f.readlines()
            BLOCKS_TO_PROCESS = [int(x) for x in CONTENT if x.strip() and len(x.strip()) <= 8]
    elif len(sys.argv) == 3:
        BLOCKS_TO_PROCESS = list(range(int(sys.argv[1]), int(sys.argv[2])))
    else:
        sys.exit("Usage: ethdrain.py <block start> <block end> OR <block list file>")

    CHUNKS = list(chunks(BLOCKS_TO_PROCESS, CHUNK_SIZE))

    print("~~Processing {} blocks split into {} chunks~~\n".format(
        len(BLOCKS_TO_PROCESS), len(CHUNKS)
    ))

    POOL = mp.Pool(POOL_SIZE)
    POOL.map(setup_process, CHUNKS)
