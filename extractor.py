#!/usr/bin/env python3
import click
from custom_elastic_search import CustomElasticSearch
from contract_transactions import ContractTransactions
from internal_transactions import InternalTransactions
from contracts import Contracts
from contract_methods import ContractMethods
from config import INDICES

def prepare_indices(host):
  elasticsearch = CustomElasticSearch(host)
  elasticsearch.prepare_fast_index(INDICES["transaction"], 'tx')
  elasticsearch.prepare_fast_index(INDICES["internal_transaction"], 'itx')

def detect_contracts(host):
  contract_transactions = ContractTransactions(INDICES, host)
  contract_transactions.detect_contract_transactions()

def extract_traces(host):
  internal_transactions = InternalTransactions(INDICES, host)
  internal_transactions.extract_traces()

def parse_inputs(host):
  contracts = Contracts(INDICES, host)
  contracts.decode_inputs()

def search_methods(host):
  contract_methods = ContractMethods(INDICES, host)
  contract_methods.search_methods()

def parse_internal_inputs(host):
  replaced_indices = {"contract": INDICES["contract"], "transaction": INDICES["internal_transaction"]}
  internal_transactions = InternalTransactions(replaced_indices, host)
  internal_transactions.extract_traces()

operations = {
  "prepare-indices": prepare_indices,
  "detect-contracts": detect_contracts,
  "extract-traces": extract_traces,
  "parse-inputs": parse_inputs,
  "parse-internal-inputs": parse_internal_inputs,
  "search-methods": search_methods
}

@click.command()
@click.option('--host', help='Elasticsearch host name', default='http://localhost:9200')
@click.option('--operation', help='Action to perform ({})'.format(", ".join(operations.keys())), default='detect-contracts')
def start_process(operation, host):
  if operation in operations.keys():
    operations[operation](host)

if __name__ == '__main__':
  start_process()
