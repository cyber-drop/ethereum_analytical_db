from custom_elastic_search import CustomElasticSearch
import requests
import json
from time import sleep
from tqdm import *

class InternalTransactions:
  def __init__(self, elasticsearch_index, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.index = elasticsearch_index
    self.client = CustomElasticSearch(elasticsearch_host)
    self.ethereum_api_host = ethereum_api_host

  def _iterate_transactions(self):
    return self.client.iterate(self.index, 'tx', 'to_contract:true AND !(_exists_:trace)')

  def _make_trace_requests(self, hashes):
    return [{
      "jsonrpc": "2.0",
      "id": id,
      "method": "trace_replayTransaction", 
      "params": [hash, ["trace"]]
    } for id, hash in hashes.items()]

  def _get_traces(self, hashes):
    request = self._make_trace_requests(hashes)
    request_string = json.dumps(request)

    responses = requests.post(
      self.ethereum_api_host, 
      data=request_string, 
      headers={"content-type": "application/json"}
    ).json()
    traces = {response["id"]: response["result"]['trace'] for response in responses}
    return traces

  def _save_traces(self, traces):
    operations = [self.client.update_op(doc={'trace': trace}, id=id) for id, trace in traces.items()]
    self.client.bulk(operations, doc_type='tx', index=self.index, refresh=True)

  def _extract_traces_chunk(self, transactions):
    hashes = {transaction["_id"]: transaction["_source"]["hash"] for transaction in transactions}
    traces = self._get_traces(hashes)
    self._save_traces(traces)

  def extract_traces(self):
    for transactions in self._iterate_transactions():
      self._extract_traces_chunk(transactions)