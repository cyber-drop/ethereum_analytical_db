from custom_elastic_search import CustomElasticSearch
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
      "method": "trace_replayBlockTransactions", 
      "params": [hex(block_number), ["trace"]]
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
    range_query = self.client.make_range_query("number", *ranges)
    return self.client.iterate(self.indices["block"], 'b', '!(_exists_:proceed) AND ' + range_query)

  def _iterate_transactions(self, blocks):
    # 'tx', 'to_contract:true AND !(_exists_:trace) AND ' + 
    transactions_query = {
      "bool": {
        "must": [
          {"term": {"to_contract": True}},
          {
            "bool": {
              "must_not": {
                "exists": {
                  "field": "trace"
                }
              }
            }
          },
          {"terms": {"blockNumber": blocks}}
        ]
      }
    }

    return self.client.iterate(self.indices["transaction"], 'tx', transactions_query)

  def _get_traces(self, blocks):    
    chunks = self._split_on_chunks(blocks, NUMBER_OF_PROCESSES)
    arguments = zip(repeat(self.parity_hosts), chunks)
    traces = self.pool.starmap(_get_traces_sync, arguments)
    return {id: trace for traces_dict in traces for id, trace in traces_dict.items()}

  def _set_trace_hashes(self, transaction, trace):
    for i, internal_transaction in enumerate(trace):
      internal_transaction["hash"] = transaction["hash"] + "." + str(i)

  def _classify_trace(self, transaction, trace):
    if "from" not in transaction.keys():
      return
    for internal_transaction in trace:
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

  def _restore_transactions_dictionary(self, blocks, transactions):
    result = {}
    for transaction in transactions:
      transaction_body = transaction["_source"]
      block_number = transaction_body["blockNumber"]
      transaction_index = transaction_body["transactionIndex"]
      result[transaction["_id"]] = blocks[str(block_number)][transaction_index]['trace']
    return result

  def _save_traces(self, traces):
    if traces:
      operations = [self.client.update_op(doc={'trace': trace}, id=id) for id, trace in traces.items()]
      self.client.bulk(operations, doc_type='tx', index=self.indices["transaction"], refresh=True)

  def _preprocess_internal_transaction(self, transaction):
    transaction = transaction.copy()
    for field in ["action", "result"]:
      if field in transaction.keys():
        transaction.update(transaction[field])
        del transaction[field]
    return transaction

  def _save_internal_transactions(self, traces):
    if traces:
      docs = [self._preprocess_internal_transaction(transaction) for trace in traces.values() for transaction in trace]
      self.client.bulk_index(docs=docs, index=self.indices["internal_transaction"], doc_type="itx", id_field="hash", refresh=True)

  def _extract_traces_chunk(self, blocks):
    blocks_traces = self._get_traces(blocks)
    for transactions in self._iterate_transactions(blocks):
      traces = self._restore_transactions_dictionary(blocks_traces, transactions)
      for transaction in transactions:
        if transaction["_id"] in traces.keys():
          transaction_body = transaction["_source"]
          trace = traces[transaction["_id"]]
          self._set_trace_hashes(transaction_body, trace)
          self._classify_trace(transaction_body, trace)
      self._save_traces(traces)
      self._save_internal_transactions(traces)

  def extract_traces(self):
    for blocks in self._iterate_blocks():
      blocks = [block["_source"]["number"] for block in blocks]
      self._extract_traces_chunk(blocks)
