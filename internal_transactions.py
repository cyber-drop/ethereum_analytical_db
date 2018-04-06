from pyelasticsearch import ElasticSearch
import requests
import json
import click

class InternalTransactions:
  def __init__(self, elasticsearch_index, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.index = elasticsearch_index
    self.client = ElasticSearch(elasticsearch_host)
    self.ethereum_api_host = ethereum_api_host

  def get_transactions_to_contracts(self):
    return self.client.search('input:0x?*', index=self.index, doc_type='tx')['hits']['hits']

  def _make_trace_request(self, hash):
    return {
      "jsonrpc": "2.0",
      "id": 1,
      "method": "trace_replayTransaction", 
      "params": [hash, ["trace"]]
    }

  def get_trace(self, hash):
    request = self._make_trace_request(hash)
    request_string = json.dumps(request)

    response = requests.post(
      self.ethereum_api_host, 
      data=request_string, 
      headers={"content-type": "application/json"}
    ).json()['result']['trace']

    return response

  def save_trace(self, transaction_id, trace):
    self.client.update(self.index, 'tx', transaction_id, doc={
      'trace': trace
    })

  def extract_traces(self, transactions):
    for transaction in transactions:
      transaction_id = transaction["_id"]
      transaction_hash = transaction["_source"]["hash"]
      trace = self.get_trace(transaction_hash)
      self.save_trace(transaction_id, trace)

@click.command()
@click.option('--index', help='Elasticsearch index name', default='ethereum-transactions')
def start_process(index):
  internal_transactions = InternalTransactions(index)
  transactions = internal_transactions.get_transactions_to_contracts()
  internal_transactions.extract_traces(transactions)

if __name__ == '__main__':
  start_process()