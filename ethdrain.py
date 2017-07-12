#!/usr/bin/python3
import asyncio
import json
import logging
import multiprocessing as mp
import argparse
import requests
import aiohttp

from elasticsearch import exceptions as es_exceptions
from elasticdatastore import ElasticDatastore

logging.basicConfig(filename='error_blocks.log', level=logging.ERROR)

class Ethdrain:

    # Holds the list of datastore classes
    data_store_classes = ()

    eth_url = "http://localhost:8545"
    sem_size = 128

    def __init__(self, block_range, block):
        self.block = block
        self.block_range = block_range
        self.data_stores = list()

    @classmethod
    def load_datastore_classes(cls, *data_store_classes):
        cls.data_store_classes = data_store_classes

    @classmethod
    def launch_history_sync(cls, block_range):
        """
        This class method will instanciate Ethdrain classes (one per process)
        and then instanciate and attach every datastore available to each on them
        """
        inst = cls(block_range, 0)
        for data_class in cls.data_store_classes:
            inst.data_stores.append(data_class())
        inst.setup_process_history_sync()

    @classmethod
    def launch_continuous_sync(cls, block):
        """
        This class method will instanciate Ethdrain classes (one per process)
        and then instanciate and attach every datastore available to each on them
        """
        inst = cls([], block)
        for data_class in cls.data_store_classes:
            inst.data_stores.append(data_class())
        inst.setup_process_continuous_sync()

    def setup_process_history_sync(self):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.run_history_sync(self.block_range))
        loop.run_until_complete(future)

        # Now that everything has been "extracted", perform the "save" action
        for data_store in self.data_stores:
            msg = data_store.save()
            print("{}: {}".format(data_store.__class__.__name__, msg))

    def setup_process_continuous_sync(self):
        # print("setup_process_continuous_sync")
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(self.indexNext())
        loop.run_forever()

    async def fetch(self, session, block_nb):
        try:
            async with session.post(self.__class__.eth_url,
                                    data=Ethdrain.make_request(block_nb),
                                    headers={"content-type": "application/json"}) as response:
                for data_store in self.data_stores:
                    data_store.extract(await response.json())

        except (aiohttp.ClientError, asyncio.TimeoutError) as exception:
            logging.error("block: " + str(block_nb))
            print("Issue with block {}:\n{}\n".format(block_nb, exception))

    async def sema_fetch(self, sem, session, block_nb):
        async with sem:
            await self.fetch(session, block_nb)


    async def run_history_sync(self, block_range):
        tasks = []
        sem = asyncio.Semaphore(self.__class__.sem_size)

        # Create client session that will ensure we dont open new connection
        # per each request.
        async with aiohttp.ClientSession() as session:
            for block_nb in block_range:
                # pass Semaphore and session to every POST request
                task = asyncio.ensure_future(self.sema_fetch(sem, session, block_nb))
                tasks.append(task)

            await asyncio.gather(*tasks)

    async def fetch2(self, session, block_nb):
        # print("[fetch2] Fethcing block number #{}".format(block_nb))
        try:
            async with session.post(self.__class__.eth_url,
                                    data=Ethdrain.make_request(block_nb),
                                    headers={"content-type": "application/json"}) as response:
                return await response.json()

        except (aiohttp.ClientError, asyncio.TimeoutError) as exception:
            logging.error("block: " + str(block_nb))
            print("Issue with block {}:\n{}\n".format(block_nb, exception))

    async def indexNext(self):
        try:
            async with aiohttp.ClientSession() as session:
                response = await self.fetch2(session, self.block)
            if response["result"] != None:
                for data_store in self.data_stores:
                    data_store.actions = list()
                    data_store.extract(response)
                    msg = data_store.save()
                    print("{}: {}".format(data_store.__class__.__name__, msg))
                self.block = self.block + 1
                return asyncio.ensure_future(self.indexNext())
            else:
                # print("[indexNext] Waiting block #{} for next 5 seconds".format(self.block+1))
                await asyncio.sleep(5)
                return asyncio.ensure_future(self.indexNext())
        except (aiohttp.ClientError, asyncio.TimeoutError) as exception:
            logging.error("block: " + str(self.block))
            print("Issue with block {}:\n{}\n".format(self.block, exception))
            return asyncio.ensure_future(self.indexNext())

    @staticmethod
    def make_request(block_nb, use_hex=True):
        return json.dumps({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(block_nb) if use_hex else block_nb, True],
            "id": 1
        })


if __name__ == "__main__":

    def http_post_request(url, request):
        return requests.post(url, data=request, headers={"content-type": "application/json"}).json()


    def chunks(lst, nb_chunks=250):
        for i in range(0, len(lst), nb_chunks):
            yield lst[i:i + nb_chunks]


    # Elasticsearch maximum number of connections
    ES_MAXSIZE = 25
    # Elasticsearch default url
    ES_URL = "http://localhost:9200"
    # Ethereum RPC endpoint
    ETH_URL = "http://localhost:8545"
    # Size of multiprocessing Pool processing the chunks
    POOL_SIZE = mp.cpu_count()

    BLOCK_WAIT = 10

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start', dest='start_block', type=int,
                        help='What block to start indexing. If nothing is provided, the latest block indexed will be used.')
    parser.add_argument('-e', '--end', dest='end_block', type=int,
                        help='What block to finish indexing. If nothing is provided, the latest one will be used.')
    parser.add_argument('-f', '--file', default=None,
                        help='Use an input file, each block number on a new line.')
    parser.add_argument('-es', '--esurl', default=ES_URL,
                        help='The elasticsearch url and port. Accepts all the same parameters needed as a normal Elasticsearch client expects.')
    parser.add_argument('-m', '--esmaxsize', default=ES_MAXSIZE,
                        help='The elasticsearch max chunk size.')
    parser.add_argument('-r', '--ethrpcurl', default=ETH_URL,
                        help='The Ethereum RPC node url and port.')

    args = parser.parse_args()

    # Setup all datastores
    ElasticDatastore.config(args.esurl, args.esmaxsize)

    # Determine start block number if needed
    if not args.start_block:
        try:
            args.start_block = ElasticDatastore.request(args.esurl, index=ElasticDatastore.B_INDEX_NAME,
                                                        size=1, sort="number:desc")["hits"]["hits"][0]["_source"]["number"]

        except (es_exceptions.NotFoundError, es_exceptions.RequestError):
            args.start_block = 0
        print("Start block automatically set to: {}".format(args.start_block))

    # Determine last block number if needed
    if not args.end_block:
        args.end_block = int(http_post_request(ETH_URL,
                                               Ethdrain.make_request("latest", False))["result"]["number"], 0) - BLOCK_WAIT
        print("Last block automatically set to: {}".format(args.end_block))

    if args.file:
        with open(args.file) as f:
            CONTENT = f.readlines()
            BLOCK_LIST = [int(x) for x in CONTENT if x.strip() and len(x.strip()) <= 8]
    else:
        BLOCK_LIST = list(range(int(args.start_block), int(args.end_block)))

    CHUNKS_ARR = list(chunks(BLOCK_LIST))

    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print("~~~~~~~~~~ Ethdrain ~~~~~~~~~~")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print("Processing {} blocks split into {} chunks on {} processes:".format(
        len(BLOCK_LIST), len(CHUNKS_ARR), POOL_SIZE
    ))

    Ethdrain.eth_url = args.ethrpcurl
    Ethdrain.load_datastore_classes(ElasticDatastore)

    POOL = mp.Pool(POOL_SIZE)
    POOL.map(Ethdrain.launch_history_sync, CHUNKS_ARR)
    print("[message] Ended history sync")
    block = args.end_block + 1
    Ethdrain.launch_continuous_sync(block)
