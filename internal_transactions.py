from pyelasticsearch import ElasticSearch
import requests
import json
import click
from time import sleep

NUMBER_OF_JOBS = 100

class InternalTransactions:
  def __init__(self, elasticsearch_index, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.index = elasticsearch_index
    self.client = ElasticSearch(elasticsearch_host)
    self.ethereum_api_host = ethereum_api_host

  def _get_transactions_to_contracts(self, size=NUMBER_OF_JOBS, page=0):
    return self.client.search('to_contract:true AND !(_exists_:trace)', index=self.index, doc_type='tx', es_from=page*size, size=size)['hits']['hits']

  def _count_transactions_to_contracts(self):
    return self.client.count('to_contract:true AND !(_exists_:trace)', index=self.index, doc_type='tx')['count']

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
    transactions_count = self._count_transactions_to_contracts()
    pages = int(transactions_count / NUMBER_OF_JOBS)
    for page in range(pages):
      transactions = self._get_transactions_to_contracts(size=NUMBER_OF_JOBS)
      if len(transactions):
        self._extract_traces_chunk(transactions)

class ContractTransactions:
  def __init__(self, elasticsearch_index, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.index = elasticsearch_index
    self.client = ElasticSearch(elasticsearch_host)
    self.ethereum_api_host = ethereum_api_host

  def extract_contract_addresses(self):
    contract_transactions = self.client.search('input:0x?*', index=self.index, doc_type='tx')['hits']['hits']
    contracts = [transaction["_source"]["to"] for transaction in contract_transactions]
    # TODO add bulk
    for contract in contracts:
      self.client.index(
        self.index, 
        'contract', 
        {'address': contract},
        id=contract
      )

  def _search_transactions_by_target(self, targets):
    elasticsearch_filter = {
      "query": {
        "terms": {
          "to": targets
        }
      }
    }
    return self.client.search(elasticsearch_filter, index=self.index, doc_type='tx')['hits']['hits']

  def detect_contract_transactions(self):
    contracts = self.client.search("address:*", index=self.index, doc_type='contract')['hits']['hits']
    contracts = [contract["_source"]["address"] for contract in contracts]
    transactions_to_contracts = self._search_transactions_by_target(contracts)
    for transaction in transactions_to_contracts:
      transaction_id = transaction["_id"]
      self.client.update(
        self.index, 'tx', transaction_id, 
        doc={'to_contract': True}
      )

@click.command()
@click.option('--index', help='Elasticsearch index name', default='ethereum-transactions')
def start_process(index):
  pass

if __name__ == '__main__':
  start_process()