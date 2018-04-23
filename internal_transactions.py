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

def _make_trace_requests(hashes):
  return [{
    "jsonrpc": "2.0",
    "id": id,
    "method": "trace_replayTransaction", 
    "params": [hash, ["trace"]]
  } for id, hash in hashes.items()]

def _get_traces_sync(hashes):
  hashes = dict(hashes)
  request = _make_trace_requests(hashes)
  request_string = json.dumps(request)

  responses = requests.post(
    "http://localhost:8545", 
    data=request_string, 
    headers={"content-type": "application/json"}
  ).json()
  traces = {response["id"]: response["result"]['trace'] for response in responses if "result" in response.keys()}
  return traces

class InternalTransactions:
  def __init__(self, elasticsearch_index, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.index = elasticsearch_index
    self.client = CustomElasticSearch(elasticsearch_host)
    self.ethereum_api_host = ethereum_api_host

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

  def _get_traces(self, hashes):
    pool = Pool(processes=NUMBER_OF_PROCESSES)
    traces = pool.map(_get_traces_sync, self._split_on_chunks(hashes.items(), NUMBER_OF_PROCESSES))
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
    hashes = {transaction["_id"]: transaction["_source"]["hash"] for transaction in transactions}
    traces = self._get_traces(hashes)
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
