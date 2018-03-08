import datetime
import logging
import csv
import os
from datastore import Datastore
import pandas as pd
from collections import OrderedDict

class CSVDatastore(Datastore):

    TX_INDEX_NAME = "ethereum-transaction"
    B_INDEX_NAME = "ethereum-block"

    NAME_FILE_TRANSACTIONS = 'transactions.csv'
    NAME_FILE_BLOCKS = 'blocks.csv'


    def __init__(self):
        super().__init__()

    def extract(self, rpc_block):
        block = rpc_block["result"]
        transactions = block["transactions"]
        tx_hashes = list()
        tx_value_sum = 0

        block_nb = int(block["number"], 0)
        block_timestamp = datetime.datetime.fromtimestamp(int(block["timestamp"], 0))

        for tx in transactions:
            tx["blockNumber"] = block_nb
            tx["blockTimestamp"] = block_timestamp
            # Convert wei into ether
            tx["value"] = int(tx["value"], 0) / self.WEI_ETH_FACTOR
            for key_tx in ("gas", "gasPrice", "blockHash", "condition", "creates", 'nonce', 'publicKey', 'r', 'raw', 's', 'standardV', 'transactionIndex', 'v'): del tx[key_tx]
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
        block["_index"] = self.B_INDEX_NAME
        block["_type"] = "b"
        block["_id"] = block_nb

        self.actions.append(
            block
        )


    def save(self):
        nb_blocks = sum(act["_type"] == "b" for act in self.actions)
        nb_txs = sum(act["_type"] == "tx" for act in self.actions)

        if self.actions:
            try:
                with open(self.NAME_FILE_TRANSACTIONS, 'a', newline='') as csv_file:
                    csv_writer = csv.writer(csv_file)
                    needHeader = 0
                    if os.path.isfile(self.NAME_FILE_TRANSACTIONS):
                        if os.stat(self.NAME_FILE_TRANSACTIONS).st_size == 0: needHeader = 1
                    else:
                        needHeader = 1
                    for item in self.actions:
                        if item['_type'] == 'tx':
                            ordered_item = OrderedDict(sorted(item['_source'].items(), key=lambda t: t[0]))
                            if needHeader == 1:
                                csv_writer.writerow([''.join(str(itemField) + ';' for itemField in list(ordered_item.keys()))[:-1]])
                                needHeader -= 1
                            csv_writer.writerow([''.join(str(itemField)+';' for itemField in list(ordered_item.values()))[:-1]])
                with open(self.NAME_FILE_BLOCKS, 'a', newline='') as csv_file:
                    csv_writer = csv.writer(csv_file, quoting = csv.QUOTE_NONE,  delimiter=';', quotechar='',escapechar=' ')
                    needHeader = 0
                    if os.path.isfile(self.NAME_FILE_BLOCKS):
                        if os.stat(self.NAME_FILE_BLOCKS).st_size==0: needHeader = 1
                    else:
                        needHeader = 1
                    for item in self.actions:
                        if item['_type'] == 'b':
                            ordered_item = OrderedDict(sorted(item.items(), key=lambda t: t[0]))
                            if needHeader==1:
                                 csv_writer.writerow([''.join(str(itemField) + ';' for itemField in list(ordered_item.keys()))[:-1]])
                                 needHeader -=1
                            csv_writer.writerow([''.join(str(itemField)+';' for itemField in list(ordered_item.values()))[:-1]])
                return "{} blocks and {} transactions indexed in csv".format(
                    nb_blocks, nb_txs
                )

            except helpers.BulkIndexError as exception:
                print("Issue with {} blocks:\n{}\n".format(nb_blocks, exception))
                blocks = (act for act in self.actions if act["_type"] == "b")
                for block in blocks:
                    logging.error("block: " + str(block["_id"]))

    @staticmethod
    def request(url):
        url = 'blocks.csv'
        with open(url) as csvfile:
            for i, l in enumerate(csvfile):
                pass
        return max(pd.read_csv(url, skiprows=i - 100, sep=';', header=None)[13])+1
