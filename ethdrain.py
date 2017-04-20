#!/usr/bin/python3
import asyncio
import json
import logging
import multiprocessing as mp
import argparse
import requests
import aiohttp

from elasticsearch import exceptions as es_exceptions

import elasticdatastore

logging.basicConfig(filename='error_blocks.log', level=logging.ERROR)

# Elasticsearch maximum number of connections
ES_MAXSIZE = 25
# Elasticsearch default url
ES_URL = "http://localhost:9200"
# Ethereum RPC endpoint
ETH_URL = "http://localhost:8545"
# Parallel processing semaphore size
SEM_SIZE = 256
# Size of chunk size in blocks
CHUNK_SIZE = 250
# Size of multiprocessing Pool processing the chunks
POOL_SIZE = mp.cpu_count() + 2


def chunks(lst, nb_chunks):
    for i in range(0, len(lst), nb_chunks):
        yield lst[i:i + nb_chunks]


def make_request(block, use_hex=True):
    return json.dumps({
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(block) if use_hex else block, True],
        "id": 1
    })

def http_post_request(url, request):
    return requests.post(url, data=request, headers={"content-type": "application/json"}).json()


async def fetch(url, session, block, process_fn):
    try:
        async with session.post(url, data=make_request(block), headers={"content-type": "application/json"}) as response:
            process_fn(await response.json())

    except (aiohttp.ClientError, asyncio.TimeoutError) as exception:
        logging.error("block: " + str(block))
        print("Issue with block {}:\n{}\n".format(block, exception))


async def sema_fetch(sem, url, session, block, process_fn):
    async with sem:
        await fetch(url, session, block, process_fn)


async def run(block_range, process_fn):
    tasks = []
    sem = asyncio.Semaphore(SEM_SIZE)

    # Create client session that will ensure we dont open new connection
    # per each request.
    async with aiohttp.ClientSession() as session:
        for i in block_range:
            # pass Semaphore and session to every POST request
            task = asyncio.ensure_future(sema_fetch(sem, ETH_URL, session, i, process_fn))
            tasks.append(task)

        await asyncio.gather(*tasks)


def setup_process(block_range):

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(block_range, ELASTIC.extract))
    loop.run_until_complete(future)

    ELASTIC.save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--start', dest='start_block', type=int,
                        help='What block to start indexing. If nothing is provided, the latest block indexed will be used.')
    parser.add_argument('-e', '--end', dest='end_block', type=int,
                        help='What block to finish indexing. If nothing is provided, the latest one will be used.')
    parser.add_argument('-f', '--file', default=None,
                        help='Use an input file, each block number on a new line.')
    parser.add_argument('-u', '--esurl', default=ES_URL,
                        help='The elasticsearch url and port. Accepts all the same parameters needed as a normal Elasticsearch client expects.')
    parser.add_argument('-m', '--esmaxsize', default=ES_MAXSIZE, help='The elasticsearch max chunk size.')
    parser.add_argument('-r', '--ethrpcurl', default=ETH_URL, help='The Ethereum RPC node url and port.')
    args = parser.parse_args()

    # Setup all datastores
    ELASTIC = elasticdatastore.ElasticDatastore(ES_URL, ES_MAXSIZE)

    # Determine start block number if needed
    if not args.start_block:
        try:
            args.start_block = ELASTIC.request(index=ELASTIC.B_INDEX_NAME,
                                               size=1, sort="number:desc")["hits"]["hits"][0]["_source"]["number"]

        except (es_exceptions.NotFoundError, es_exceptions.RequestError):
            args.start_block = 0
        print("Start block automatically set to: {}".format(args.start_block))

    # Determine last block number if needed
    if not args.end_block:
        args.end_block = int(http_post_request(ETH_URL, make_request("latest", False))["result"]["number"], 0) + 1
        print("Last block automatically set to: {}".format(args.end_block))

    if args.file:
        with open(args.file) as f:
            CONTENT = f.readlines()
            block_list = [int(x) for x in CONTENT if x.strip() and len(x.strip()) <= 8]
    else:
        block_list = list(range(int(args.start_block), int(args.end_block)))

    ES_MAXSIZE = int(args.esmaxsize)
    ES_URL = args.esurl
    ETH_URL = args.ethrpcurl

    chunks_arr = list(chunks(block_list, CHUNK_SIZE))

    print("~~Processing {} blocks split into {} chunks~~\n".format(
        len(block_list), len(chunks_arr)
    ))

    POOL = mp.Pool(POOL_SIZE)
    POOL.map(setup_process, chunks_arr)
