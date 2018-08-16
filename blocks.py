from config import INDICES, PARITY_HOSTS
from custom_elastic_search import CustomElasticSearch
import requests
import json
import utils
from tqdm import tqdm
from web3 import Web3, HTTPProvider
import datetime

BLOCKS_PER_CHUNK = 10000

class Blocks:
  def __init__(self,
               indices=INDICES,
               elasticsearch_host="http://localhost:9200",
               parity_host=PARITY_HOSTS[0][-1]
               ):
    self.indices = indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.parity_host = parity_host
    self.w3 = Web3(HTTPProvider('http://localhost:8550'))

  def _get_max_parity_block(self):
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
    aggregation = {
      "size": 0,
      "aggs": {
        "max_block": {
          "max": {
            "field": "number"
          }
        }
      }
    }
    result = self.client.send_request("GET", [self.indices["block"], "_search"], aggregation, {})
    max_block = result["aggregations"]["max_block"]["value"]
    if max_block:
      return int(max_block)
    else:
      return 0

  def _extract_block_timestamp(self, block_number):
    block = self.w3.eth.getBlock(block_number)
    if block != None:
      timestamp = block.timestamp
      return datetime.datetime.fromtimestamp(timestamp)

  def _create_blocks(self, start, end):
    docs = [{
      "number": i,
      'id': i,
      'timestamp': self._extract_block_timestamp(i)
    } for i in range(start, end + 1)]
    if docs:
      for chunk in tqdm(list(utils.split_on_chunks(docs, BLOCKS_PER_CHUNK))):
        self.client.bulk_index(docs=chunk, index=self.indices["block"], doc_type="b", refresh=True)

  def create_blocks(self):
    max_parity_block = self._get_max_parity_block()
    max_elasticsearch_block = self._get_max_elasticsearch_block()
    self._create_blocks(max_elasticsearch_block + 1, max_parity_block)
