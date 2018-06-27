from custom_elastic_search import CustomElasticSearch, NUMBER_OF_JOBS
from pyelasticsearch.exceptions import BulkError
import requests
import json
from time import sleep
from tqdm import *
from multiprocessing import Pool
from functools import partial
from itertools import repeat
from config import PARITY_HOSTS
import pygtrie as trie
import utils
from pyelasticsearch import bulk_chunks

BYTES_PER_CHUNK = 1000000
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
  traces = []
  for parity_url, request in requests_dict.items():
    request_string = json.dumps(request)
    responses = requests.post(
      parity_url,
      data=request_string,
      headers={"content-type": "application/json"}
    ).json()
    for response in responses:
      if "result" in response.keys():
        traces += response["result"]
  return traces

class InternalTransactions:
  def __init__(self, elasticsearch_indices, elasticsearch_host="http://localhost:9200", parity_hosts=PARITY_HOSTS):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.pool = Pool(processes=NUMBER_OF_PROCESSES)
    self.parity_hosts = parity_hosts

  def _split_on_chunks(self, iterable, size):
    return utils.split_on_chunks(iterable, size)

  def _iterate_blocks(self):

    ranges = [host_tuple[0:2] for host_tuple in self.parity_hosts]
    range_query = self.client.make_range_query("number", *ranges)
    query = {
      "query_string": {
        "query": '!(_exists_:traces_extracted) AND ' + range_query
      }
    }
    return self.client.iterate(self.indices["block"], "b", query)

  def _get_traces(self, blocks):
    chunks = self._split_on_chunks(blocks, NUMBER_OF_PROCESSES)
    arguments = list(zip(repeat(self.parity_hosts), chunks))
    traces = self.pool.starmap(_get_traces_sync, arguments)
    return [transaction for trace in traces for transaction in trace]

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

  def _set_parent_errors(self, trace):
    errors = {}
    for transaction in trace:
      if "error" in transaction.keys():
        if transaction["transactionHash"] not in errors.keys():
          errors[transaction["transactionHash"]] = trie.Trie()
        errors[transaction["transactionHash"]][transaction["traceAddress"]] = True
    for transaction in trace:
      if transaction["transactionHash"] in errors.keys():
        prefix_exists = bool(errors[transaction["transactionHash"]].shortest_prefix(transaction["traceAddress"]))
        is_node = errors[transaction["transactionHash"]].has_key(transaction["traceAddress"])
        if prefix_exists and not is_node:
          transaction["parent_error"] = True

  # TODO get rid of this method
  def _classify_trace(self, trace):
    transactions_dict = {
      transaction["hash"][:-2]: transaction for transaction in trace if transaction["hash"].endswith(".0")
    }
    for internal_transaction in trace:
      transaction_hash = internal_transaction["transactionHash"]
      if not transaction_hash or (transaction_hash not in transactions_dict.keys()) or (transaction_hash.endswith(".0")):
        continue
      transaction = transactions_dict[transaction_hash]["action"]
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

  def _save_traces(self, blocks):
    query = {
      "terms": {
        "number": blocks
      }
    }
    self.client.update_by_query(self.indices["block"], 'b', query, 'ctx._source.traces_extracted = true')

  def _preprocess_internal_transaction(self, transaction):
    transaction = transaction.copy()
    for field in ["action", "result"]:
      if (field in transaction.keys()) and (transaction[field]):
        transaction.update(transaction[field])
        del transaction[field]
    if "value" in transaction.keys():
      if transaction["value"] == "0x":
        transaction["value"] = 0
      else:
        transaction["value"] = int(transaction["value"], 0) / 1e18
    return transaction

  def _save_internal_transactions(self, blocks_traces):
    docs = [
      self._preprocess_internal_transaction(transaction)
      for transaction in blocks_traces
      if transaction["transactionHash"]
    ]
    if docs:
      for chunk in bulk_chunks(docs, None, BYTES_PER_CHUNK):
        self.client.bulk_index(docs=chunk, index=self.indices["internal_transaction"], doc_type="itx", id_field="hash", refresh=True)

  def _save_miner_transactions(self, blocks_traces):
    docs = [self._preprocess_internal_transaction(transaction) for transaction in blocks_traces if not transaction["transactionHash"]]
    self.client.bulk_index(docs=docs, index=self.indices["miner_transaction"], doc_type="tx", id_field="hash",
                           refresh=True)

  def _save_transactions_error(self, blocks_traces):
    operations = [self.client.update_op(
      doc={"error": transaction["error"]},
      id=transaction["transactionHash"]
    ) for transaction in blocks_traces
      if ("error" in transaction.keys()) and ("transactionHash" in transaction.keys()) and (transaction.get("hash", "").endswith(".0"))]
    if operations:
      try:
        self.client.bulk(operations, index=self.indices["transaction"], doc_type="tx", refresh=True)
      except BulkError:
        pass

  def _save_transactions_output(self, blocks_traces):
    operations = [self.client.update_op(
      doc={"output": transaction["result"]["output"]},
      id=transaction["transactionHash"]
    ) for transaction in blocks_traces
      if ("result" in transaction.keys())
         and (transaction["result"])
         and ("output" in transaction["result"].keys())
         and ("transactionHash" in transaction.keys())
         and (transaction["hash"].endswith(".0"))]
    if operations:
      try:
        self.client.bulk(operations, index=self.indices["transaction"], doc_type="tx", refresh=True)
      except BulkError:
        pass

  def _extract_traces_chunk(self, blocks):
    blocks_traces = self._get_traces(blocks)
    self._set_trace_hashes(blocks_traces)
    self._set_parent_errors(blocks_traces)
    self._save_internal_transactions(blocks_traces)
    self._save_miner_transactions(blocks_traces)
    self._save_traces(blocks)

  def extract_traces(self):
    for blocks in self._iterate_blocks():
      blocks = [block["_source"]["number"] for block in blocks]
      self._extract_traces_chunk(blocks)
