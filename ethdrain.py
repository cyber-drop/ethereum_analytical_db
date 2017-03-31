#!/usr/bin/python3
import sys
import asyncio
import json
import datetime
import aiohttp
from multiprocessing import Pool

from aiohttp import ClientSession
from elasticsearch import Elasticsearch
from elasticsearch import helpers

TX_INDEX_NAME = "ethereum-transaction"
B_INDEX_NAME  = "ethereum-block"
HTTP_HEADERS = { "content-type": "application/json" }

# Elasticsearch maximum number of connections
ES_MAXSIZE = 10
# Parallel processing semaphore size
SEM_SIZE   = 256
# Size of chunk size in blocks
CHUNK_SIZE = 500
# Size of multiprocessing Pool processing the chunks
POOL_SIZE  = 8

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def makeRequest(id):
    return json.dumps({
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(id), True],
        "id": 1
    })

async def fetch(url, session, blockNb, process, actions):
    try:
        async with session.post(url, data=makeRequest(blockNb), headers=HTTP_HEADERS) as response:
            data = await response.json()
    except:
        sys.stderr.write(str(blockNb) + "\n")

    if data:
        process(data, actions)

async def sema_fetch(sem, url, session, blockNb, fn, actions):
    async with sem:
        await fetch(url, session, blockNb, fn, actions)

async def run(blockRange, processFn, actions):
    url = "http://localhost:8545"
    tasks = []
    sem = asyncio.Semaphore(SEM_SIZE)

    # Create client session that will ensure we dont open new connection
    # per each request.
    async with ClientSession() as session:
        for i in blockRange:
            # pass Semaphore and session to every POST request
            task = asyncio.ensure_future(sema_fetch(sem, url, session, i, processFn, actions))
            tasks.append(task)

        await asyncio.gather(*tasks)

def process_block(b, actions):
    b = b["result"]

    txs = b["transactions"]
    txHashes = list()
    txValueSum = 0

    blockNb = int(b["number"], 0)
    blockTimestamp = datetime.datetime.fromtimestamp(int(b["timestamp"], 0))

    if(len(txs) > 0):
        for tx in txs:
            tx["blockNumber"] = int(tx["blockNumber"], 0)
            tx["blockTimestamp"] = blockTimestamp
            # Convert wei into ether
            tx["value"] = int(tx["value"], 0) / 1000000000000000000.0
            txValueSum += tx["value"]
            actions.append({ "_index":TX_INDEX_NAME, "_type":"tx", "_id":tx["hash"], "_source":tx })
            txHashes.append(tx["hash"])

    b["transactions"] = txHashes
    b["number"] = blockNb
    b["timestamp"] = blockTimestamp
    b["gasLimit"] = int(b["gasLimit"], 0)
    b["gasUsed"] = int(b["gasUsed"], 0)
    b["size"] = int(b["size"], 0)
    b["transactionCount"] = len(txHashes)
    b["txValueSum"] = txValueSum

    actions.append({ "_index": B_INDEX_NAME, "_type":"b", "_id":blockNb, "_source":b })

def setup_process(block_range):

    out_actions = list()

    es = Elasticsearch(["localhost"], maxsize=ES_MAXSIZE)

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(block_range, process_block, out_actions))
    loop.run_until_complete(future)

    blocks = [act for act in out_actions if act["_type"] == "b"]
    txs    = [act for act in out_actions if act["_type"] == "tx"]

    try:
        helpers.bulk(es, out_actions)
        print("#{}: ({}b, {}tx)".format(max([int(b["_id"]) for b in blocks]), len(blocks), len(txs)))
    except:
        for act in blocks:
            sys.stderr.write(str(act["_id"]) + "\n")

if __name__ == "__main__":

    if len(sys.argv) == 2:
        with open(sys.argv[1]) as f:
            content = f.readlines()
            blockRange = [int(x) for x in content if len(x.strip()) > 0 and len(x.strip()) <= 8]
    elif len(sys.argv) == 3:
        blockRange = range(int(sys.argv[1]), int(sys.argv[2]))
    else:
        sys.exit("Usage: ethdrain.py <block start> <block end> OR <block list file>")

    chks = chunks(blockRange, CHUNK_SIZE)

    p = Pool(POOL_SIZE)
    p.map(setup_process, chks)

