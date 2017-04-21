import datetime
import logging
from datastore import Datastore

from elasticsearch import Elasticsearch
from elasticsearch import helpers

class ElasticDatastore(Datastore):

    TX_INDEX_NAME = "ethereum-transaction"
    B_INDEX_NAME = "ethereum-block"

    def __init__(self):
        super().__init__()
        self.elastic = Elasticsearch([self.es_url], maxsize=self.es_maxsize,
                                     timeout=30, max_retries=10, retry_on_timeout=True)

    @classmethod
    def config(cls, es_url, es_maxsize):
        cls.es_url = es_url
        cls.es_maxsize = es_maxsize


    def extract(self, rpc_block):
        block = rpc_block["result"]

        transactions = block["transactions"]
        tx_hashes = list()
        tx_value_sum = 0

        block_nb = int(block["number"], 0)
        block_timestamp = datetime.datetime.fromtimestamp(int(block["timestamp"], 0))

        for tx in transactions:
            tx["blockNumber"] = int(tx["blockNumber"], 0)
            tx["blockTimestamp"] = block_timestamp
            # Convert wei into ether
            tx["value"] = int(tx["value"], 0) / self.WEI_ETH_FACTOR
            tx_value_sum += tx["value"]
            self.actions.append(
                {"_index": self.TX_INDEX_NAME, "_type": "tx", "_id": tx["hash"], "_source": tx}
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

        self.actions.append(
            {"_index": self.B_INDEX_NAME, "_type": "b", "_id": block_nb, "_source": block}
        )


    def save(self):
        nb_blocks = sum(act["_type"] == "b" for act in self.actions)
        nb_txs = sum(act["_type"] == "tx" for act in self.actions)

        if self.actions:
            try:
                helpers.bulk(self.elastic, self.actions)
                return "{} blocks and {} transactions indexed".format(
                    nb_blocks, nb_txs
                )

            except helpers.BulkIndexError as exception:
                print("Issue with {} blocks:\n{}\n".format(nb_blocks, exception))
                blocks = (act for act in self.actions if act["_type"] == "b")
                for block in blocks:
                    logging.error("block: " + str(block["_id"]))


    @staticmethod
    def request(url, **kwargs):
        return Elasticsearch([url]).search(**kwargs)
