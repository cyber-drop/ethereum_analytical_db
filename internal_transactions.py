from pyelasticsearch import ElasticSearch
import requests
import json
import click
from time import sleep
from tqdm import *

NUMBER_OF_JOBS = 100

def elasticsearch_iterate(client, index, doc_type, query, per=NUMBER_OF_JOBS):
  items_count = client.count(query, index=index, doc_type=doc_type)['count']
  pages = round(items_count / per + 0.4999)
  for page in range(pages):
    page_items = client.search(query, index=index, doc_type=doc_type, size=per)['hits']['hits']
    yield page_items

class InternalTransactions:
  def __init__(self, elasticsearch_index, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.index = elasticsearch_index
    self.client = ElasticSearch(elasticsearch_host)
    self.ethereum_api_host = ethereum_api_host

  def _iterate_transactions(self):
    return elasticsearch_iterate(self.client, self.index, 'tx', 'to_contract:true AND !(_exists_:trace)')

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

class ContractTransactions:
  def __init__(self, elasticsearch_index, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.index = elasticsearch_index
    self.client = ElasticSearch(elasticsearch_host)
    self.ethereum_api_host = ethereum_api_host

  def _count_transactions_to_contracts(self):
    return self.client.count('input:0x?*', index=self.index, doc_type='tx')['count']

  def extract_contract_addresses(self):
    # contract_transactions_count = self._count_transactions_to_contracts()
    # pages = int(contract_transactions_count / NUMBER_OF_JOBS)
    contract_transactions = self.client.search('input:0x?*', index=self.index, doc_type='tx')['hits']['hits']
    contracts = [transaction["_source"]["to"] for transaction in contract_transactions]
    for contract in tqdm(contracts):
      self.client.index(
        self.index, 
        'contract', 
        {'address': contract},
        id=contract,
        refresh=True
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
    for transaction in tqdm(transactions_to_contracts):
      transaction_id = transaction["_id"]
      self.client.update(
        self.index, 'tx', transaction_id, 
        doc={'to_contract': True},
        refresh=True
      )

@click.command()
@click.option('--index', help='Elasticsearch index name', default='ethereum-transaction')
def start_process(index):
  contract_transactions = ContractTransactions(index)
  internal_transactions = InternalTransactions(index)

  contract_transactions.extract_contract_addresses()
  contract_transactions.detect_contract_transactions()
  internal_transactions.extract_traces()


if __name__ == '__main__':
  start_process()