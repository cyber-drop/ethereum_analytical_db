#!/usr/bin/env python3
import click
from custom_elastic_search import CustomElasticSearch
from contract_transactions import InternalContractTransactions, ExternalContractTransactions, ContractTransactions
from internal_transactions import InternalTransactions
from contracts import InternalContracts, ExternalContracts, Contracts
from contract_methods import ContractMethods
from token_holders import TokenHolders
from config import INDICES

def prepare_indices(host):
  elasticsearch = CustomElasticSearch(host)
  elasticsearch.prepare_fast_index(INDICES["transaction"], 'tx')
  elasticsearch.prepare_fast_index(INDICES["internal_transaction"], 'itx')

def detect_contracts(host):
  contract_transactions = ExternalContractTransactions(INDICES, host)
  contract_transactions.extract_contract_addresses()

def detect_internal_contracts(host):
  contract_transactions = InternalContractTransactions(INDICES, host)
  contract_transactions.extract_contract_addresses()

def detect_contract_transactions(host):
  contract_transactions = ContractTransactions(INDICES, host)
  contract_transactions.detect_contract_transactions()

def extract_traces(host):
  internal_transactions = InternalTransactions(INDICES, host)
  internal_transactions.extract_traces()

def extract_contracts_abi(host):
  contracts = Contracts(INDICES, host)
  contracts.save_contracts_abi()

def parse_inputs(host):
  contracts = ExternalContracts(INDICES, host)
  contracts.decode_inputs()

def parse_internal_inputs(host):
  internal_transactions = InternalContracts(INDICES, host)
  internal_transactions.decode_inputs()

def search_methods(host):
  contract_methods = ContractMethods(INDICES, host)
  contract_methods.search_methods()

def extract_tokens_txs(host):
  token_holders = TokenHolders(INDICES, host)
  token_holders.get_listed_tokens_txs()

operations = {
  "prepare-indices": prepare_indices,
  "detect-contracts": detect_contracts,
  "detect-internal-contracts": detect_internal_contracts,
  "detect-contract-transactions": detect_contract_transactions,
  "extract-traces": extract_traces,
  "extract-contracts-abi": extract_contracts_abi,
  "parse-inputs": parse_inputs,
  "parse-internal-inputs": parse_internal_inputs,
  "search-methods": search_methods,
  "extract-tokens-txs": extract_tokens_txs
}

@click.command()
@click.option('--host', help='Elasticsearch host name', default='http://localhost:9200')
@click.option('--operation', help='Action to perform ({})'.format(", ".join(operations.keys())), default='prepare-indices')
def start_process(operation, host):
  if operation in operations.keys():
    operations[operation](host)

if __name__ == '__main__':
  start_process()
