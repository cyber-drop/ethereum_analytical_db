from custom_elastic_search import CustomElasticSearch, NUMBER_OF_JOBS
import requests
import json
from time import sleep
from tqdm import *
from multiprocessing import Pool
from functools import partial
from itertools import repeat
from config import PARITY_HOSTS

NUMBER_OF_PROCESSES = 10

INPUT_TRANSACTION = 0
INTERNAL_TRANSACTION = 1
OUTPUT_TRANSACTION = 2
OTHER_TRANSACTION = 3

MAX_BLOCKS_NUMBER = 10000000

def _get_parity_url_by_block(parity_hosts, block):
  for bottom_line, upper_bound, url in parity_hosts:
    if ((not bottom_line) or (block >= bottom_line)) and ((not upper_bound) or (block < upper_bound)):
      return url

def _make_trace_requests(parity_hosts, blocks):
  requests = {}
  for block_number in blocks:
    parity_url = _get_parity_url_by_block(parity_hosts, block_number)
    if parity_url not in requests.keys():
      requests[parity_url] = []
    requests[parity_url].append({
      "jsonrpc": "2.0",
      "id": block_number,
      "method": "trace_block",
      "params": [hex(block_number)]
    }) 
  return requests

def _get_traces_sync(parity_hosts, blocks):
  requests_dict = _make_trace_requests(parity_hosts, blocks)
  traces = {}
  for parity_url, request in requests_dict.items():
    request_string = json.dumps(request)
    responses = requests.post(
      parity_url,
      data=request_string,
      headers={"content-type": "application/json"}
    ).json()
    traces.update({str(response["id"]): response["result"] for response in responses if "result" in response.keys()})
  return traces

class InternalTransactions:
  def __init__(self, elasticsearch_indices, elasticsearch_host="http://localhost:9200", parity_hosts=PARITY_HOSTS):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.pool = Pool(processes=NUMBER_OF_PROCESSES)
    self.parity_hosts = parity_hosts

  def _split_on_chunks(self, iterable, size):
    iterable = iter(iterable)
    for element in iterable:
      elements = [element]
      try:
        for i in range(size - 1):
          elements.append(next(iterable))
      except StopIteration:
        pass
      yield elements

  def _iterate_blocks(self):
    ranges = [host_tuple[0:2] for host_tuple in self.parity_hosts]
    range_query = self.client.make_range_query("blockNumber", *ranges)
    query = {
      "size": 0,
      "query": {
        "query_string": {
          "query": '!(_exists_:trace) AND ' + range_query
        }
      },
      "aggs": {
        "blocks": {
          "terms": {"field": "blockNumber", "size": MAX_BLOCKS_NUMBER}
        }
      }
    }
    result = self.client.search(index=self.indices["transaction"], doc_type='tx', query=query)
    blocks = [bucket["key"] for bucket in result["aggregations"]["blocks"]["buckets"]]
    return blocks

  def _iterate_transactions(self, block):
    return self.client.iterate(self.indices["transaction"], 'tx', "to_contract:true AND blockNumber:" + str(block))

  def _get_traces(self, blocks):    
    chunks = self._split_on_chunks(blocks, NUMBER_OF_PROCESSES)
    arguments = list(zip(repeat(self.parity_hosts), chunks))
    traces = self.pool.starmap(_get_traces_sync, arguments)
    return {id: trace for traces_dict in traces for id, trace in traces_dict.items()}

  def _set_trace_hashes(self, trace):
    traces_size = {}
    for transaction in trace:
      transaction_hash = transaction["transactionHash"]
      if transaction_hash:
        if transaction_hash not in traces_size.keys():
          traces_size[transaction_hash] = 0
        transaction["hash"] = "{}.{}".format(transaction["transactionHash"], traces_size[transaction_hash])
        traces_size[transaction_hash] += 1
      else:
        transaction["hash"] = transaction["blockHash"]

  def _classify_trace(self, transactions, trace):
    transactions_dict = {
      transaction["_id"]: transaction for transaction in transactions
    }
    for internal_transaction in trace:
      transaction_hash = internal_transaction["transactionHash"]
      if not transaction_hash or (transaction_hash not in transactions_dict.keys()):
        continue
      transaction = transactions_dict[transaction_hash]["_source"]
      action = internal_transaction["action"]
      if ("from" not in action.keys()) or ("to" not in action.keys()):
        internal_transaction["class"] = OTHER_TRANSACTION
        continue
      if (action["from"] == transaction["from"]) and (action["to"] == transaction["to"]):
        internal_transaction["class"] = INPUT_TRANSACTION
      elif (action["from"] == transaction["to"]) and (action["to"] == transaction["from"]):
        internal_transaction["class"] = INTERNAL_TRANSACTION
      elif (action["from"] == transaction["from"]) and (action["to"] != action["from"]):
        internal_transaction["class"] = OUTPUT_TRANSACTION
      else:
        internal_transaction["class"] = OTHER_TRANSACTION

  def _set_block_number(self, trace, block):
    for transaction in trace:
      transaction["blockNumber"] = block

  def _save_traces(self, blocks):
    transactions_query = {
      "terms": {
        "blockNumber": blocks
      }
    }
    self.client.update_by_query(self.indices["transaction"], 'tx', transactions_query, 'ctx._source.trace = true')

  def _preprocess_internal_transaction(self, transaction):
    transaction = transaction.copy()
    for field in ["action", "result"]:
      if (field in transaction.keys()) and (transaction[field]):
        transaction.update(transaction[field])
        del transaction[field]
    return transaction

  def _save_internal_transactions(self, transactions):
    docs = [self._preprocess_internal_transaction(transaction) for transaction in transactions if transaction["transactionHash"]]
    if docs:
      self.client.bulk_index(docs=docs, index=self.indices["internal_transaction"], doc_type="itx", id_field="hash", refresh=True)

  def _save_miner_transactions(self, transactions):
    docs = [self._preprocess_internal_transaction(transaction) for transaction in transactions if not transaction["transactionHash"]]
    self.client.bulk_index(docs=docs, index=self.indices["miner_transaction"], doc_type="tx", id_field="hash",
                           refresh=True)

  def _extract_traces_chunk(self, blocks):
    blocks_traces = self._get_traces(blocks)
    for block, trace in blocks_traces.items():
      self._set_trace_hashes(trace)
      self._set_block_number(trace, block)
      for transactions in self._iterate_transactions(block):
        self._classify_trace(transactions, trace)
      self._save_internal_transactions(trace)
      self._save_miner_transactions(trace)
    self._save_traces(blocks)

  def extract_traces(self):
    blocks_chunks = list(self._split_on_chunks(self._iterate_blocks(), NUMBER_OF_JOBS))
    for blocks in tqdm(blocks_chunks):
      self._extract_traces_chunk(blocks)
