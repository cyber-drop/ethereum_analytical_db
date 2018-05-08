import click
from custom_elastic_search import CustomElasticSearch
from contract_transactions import ContractTransactions
from internal_transactions import InternalTransactions
from contracts import Contracts
from contract_methods import ContractMethods

@click.command()
@click.option('--host', help='Elasticsearch host name', default='http://localhost:9200')
@click.option('--index', help='Elasticsearch index name', default='ethereum-transaction')
@click.option('--operation', help='Action to perform (detect-contracts, extract-traces, parse-inputs)', default='detect-contracts')
def start_process(index, operation, host):
  elasticsearch = CustomElasticSearch(host)
  contract_transactions = ContractTransactions(index, host)
  internal_transactions = InternalTransactions(index, host)
  contracts = Contracts(index, host)
  contract_methods = ContractMethods(index, host)
  if operation == "prepare-index":
    elasticsearch.prepare_fast_index(index)
  elif operation == "detect-contracts":
    contract_transactions.detect_contract_transactions()
  elif operation == "extract-traces":
    internal_transactions.extract_traces()
  elif operation == "parse-inputs":
    contracts.decode_inputs()
  elif operation == "search-methods":
    contract_methods.search_methods()

if __name__ == '__main__':
  start_process()
