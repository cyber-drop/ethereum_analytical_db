from config import INDICES, PARITY_HOSTS, NUMBER_OF_JOBS, ETHEREUM_START_DATE
from clients.custom_clickhouse import CustomClickhouse
import requests
import json
import utils
from tqdm import tqdm
from web3 import Web3, HTTPProvider
import datetime

BLOCKS_PER_CHUNK = NUMBER_OF_JOBS


class Blocks:
    def __init__(self, indices, client, parity_host):
        self.indices = indices
        self.client = client
        self.parity_host = parity_host
        self.w3 = Web3(HTTPProvider(parity_host))

    def _get_max_parity_block(self):
        """
        Get last block in parity

        Returns
        -------
        int:
            Last block number
            0 if there are no blocks in parity
        """
        response = requests.post(self.parity_host, data=json.dumps({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": []
        }), headers={
            "Content-Type": "application/json"
        }).json()
        return int(response["result"], 0)

    def _get_max_elasticsearch_block(self):
        """
        Get last block in ElasticSearch

        Returns
        -------
        int:
            Last block number
            0 if there are no blocks in ElasticSearch
        """
        max_block = self.client.send_sql_request('SELECT max(number) FROM {}'.format(self.indices["block"]))
        if max_block:
            return int(max_block)
        else:
            return -1

    def _extract_block_timestamp(self, block_number):
        """
        Get block timestamp from parity

        Parameters
        ----------
        block_number : int
            Block number

        Returns
        -------
        datetime
            Timestamp of a block
            None if no such block in parity
        """
        if block_number == 0:
            return ETHEREUM_START_DATE
        block = self.w3.eth.getBlock(block_number)
        if block != None:
            timestamp = block.timestamp
            return datetime.datetime.fromtimestamp(timestamp)

    def _create_blocks(self, start, end):
        """
        Create blocks from start to end. Extract timestamps for each block

        Parameters
        ----------
        start : int
            Start block number
        end : int
            End block number
        """
        docs = [{
            "number": i,
            'id': i,
        } for i in range(start, end + 1)]
        if docs:
            for chunk in tqdm(list(utils.split_on_chunks(docs, BLOCKS_PER_CHUNK))):
                for doc in chunk:
                    doc.update({'timestamp': self._extract_block_timestamp(doc['number'])})
                self.client.bulk_index(docs=chunk, index=self.indices["block"], doc_type="b", refresh=True)

    def create_blocks(self):
        """
        Create blocks from last block in ElasticSearch to last parity block in ElasticSearch

        This function is an entry point for prepare-blocks operation
        """
        max_parity_block = self._get_max_parity_block()
        max_elasticsearch_block = self._get_max_elasticsearch_block()
        self._create_blocks(max_elasticsearch_block + 1, max_parity_block)


class ClickhouseBlocks(Blocks):
    def __init__(self, indices=INDICES, parity_host=PARITY_HOSTS[0][-1]):
        super().__init__(indices, CustomClickhouse(), parity_host)
