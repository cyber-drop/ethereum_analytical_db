from custom_elastic_search import CustomElasticSearch
import requests
import json
from time import sleep
from tqdm import *
from multiprocessing import Pool

NUMBER_OF_PROCESSES = 10
INPUT_TRANSACTION = 0
INTERNAL_TRANSACTION = 1
OUTPUT_TRANSACTION = 2
OTHER_TRANSACTION = 3
PARITY_HOSTS = [
  (0, 2e6, "http://localhost:8545")
]

def _get_parity_url_by_block(block):
  for bottom_line, upper_bound, url in PARITY_HOSTS:
    if ((not bottom_line) or (block >= bottom_line)) and ((not upper_bound) or (block < upper_bound)):
      return url

def _make_trace_requests(transactions):
  requests = {}
  for transaction_id, transaction in transactions.items():
    request_url = _get_parity_url_by_block(transaction["block"])
    if not request_url:
      continue
    if request_url not in requests.keys():
      requests[request_url] = []
    requests[request_url].append({
      "jsonrpc": "2.0",
      "id": transaction_id,
      "method": "trace_replayTransaction", 
      "params": [transaction["hash"], ["trace"]]
    })
  return requests

def _get_traces_sync(transactions):
  transactions = dict(transactions)
  requests_dict = _make_trace_requests(transactions)
  traces = {}
  for parity_url, request in requests_dict.items():
    request_string = json.dumps(request)
    responses = requests.post(
      parity_url, 
      data=request_string, 
      headers={"content-type": "application/json"}
    ).json()
    traces.update({str(response["id"]): response["result"]['trace'] for response in responses if "result" in response.keys()})
  return traces

class InternalTransactions:
  def __init__(self, elasticsearch_index, elasticsearch_host="http://localhost:9200"):
    self.index = elasticsearch_index
    self.client = CustomElasticSearch(elasticsearch_host)

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

  def _iterate_transactions(self):
    return self.client.iterate(self.index, 'tx', 'to_contract:true AND !(_exists_:trace)')

  def _get_traces(self, transactions):
    pool = Pool(processes=NUMBER_OF_PROCESSES)
    traces = pool.map(_get_traces_sync, self._split_on_chunks(transactions.items(), NUMBER_OF_PROCESSES))
    return {id: trace for traces_dict in traces for id, trace in traces_dict.items()}

  def _set_trace_hashes(self, transaction, trace):
    for i, internal_transaction in enumerate(trace):
      internal_transaction["hash"] = transaction["hash"] + str(i)

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

  def _save_traces(self, traces):
    if traces:
      operations = [self.client.update_op(doc={'trace': trace}, id=id) for id, trace in traces.items()]
      self.client.bulk(operations, doc_type='tx', index=self.index, refresh=True)

  def _extract_traces_chunk(self, transactions):
    transactions_dict = {
      transaction["_id"]: {
        'hash': transaction["_source"]["hash"], 
        "block": transaction["_source"]["blockNumber"]
      } for transaction in transactions
    }
    traces = self._get_traces(transactions_dict)
    for transaction in transactions:
      if transaction["_id"] in traces.keys():
        transaction_body = transaction["_source"]
        trace = traces[transaction["_id"]]
        self._set_trace_hashes(transaction_body, trace)
        self._classify_trace(transaction_body, trace)
    self._save_traces(traces)

  def extract_traces(self):
    for transactions in self._iterate_transactions():
      self._extract_traces_chunk(transactions)
