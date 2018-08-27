#!/usr/bin/env python3
import click
from custom_elastic_search import CustomElasticSearch
from contract_transactions import InternalContractTransactions, ExternalContractTransactions, ContractTransactions
from internal_transactions import InternalTransactions
from contracts import InternalContracts, Contracts
from contract_methods import ContractMethods
from token_holders import ExternalTokenTransactions, InternalTokenTransactions
from config import INDICES
from token_prices import TokenPrices
from blocks import Blocks

def prepare_indices(host):
  elasticsearch = CustomElasticSearch(host)
  elasticsearch.create_index(INDICES["block"])
  elasticsearch.prepare_fast_index(INDICES["transaction"], 'tx')
  elasticsearch.prepare_fast_index(INDICES["internal_transaction"], 'itx')

def prepare_blocks(host):
  blocks = Blocks(INDICES, host)
  blocks.create_blocks()

def detect_contracts(host):
  contract_transactions = InternalContractTransactions(INDICES, host)
  contract_transactions.extract_contract_addresses()

def detect_contract_transactions(host):
  contract_transactions = InternalContractTransactions(INDICES, host)
  contract_transactions.detect_contract_transactions()

def extract_traces(host):
  internal_transactions = InternalTransactions(INDICES, host)
  internal_transactions.extract_traces()

def extract_contracts_abi(host):
  contracts = Contracts(INDICES, host)
  contracts.save_contracts_abi()

def parse_inputs(host):
  internal_transactions = InternalContracts(INDICES, host)
  internal_transactions.decode_inputs()

def search_methods(host):
  contract_methods = ContractMethods(INDICES, host)
  contract_methods.search_methods()

def extract_token_transactions(host):
  token_holders = InternalTokenTransactions(INDICES, host)
  token_holders.get_listed_tokens_txs()

def extract_prices(host):
  token_prices = TokenPrices(INDICES, host)
  token_prices.get_prices_within_interval()

operations = {
  "prepare-indices": prepare_indices,
  "prepare-blocks": prepare_blocks,
  "detect-contracts": detect_contracts,
  "detect-contract-transactions": detect_contract_transactions,
  "extract-traces": extract_traces,
  "extract-contracts-abi": extract_contracts_abi,
  "parse-inputs": parse_inputs,
  "search-methods": search_methods,
  "extract-token-transactions": extract_token_transactions,
  "extract-prices": extract_prices,
}

@click.command()
@click.option('--host', help='Elasticsearch host name', default='http://localhost:9200')
@click.option('--operation', help='Action to perform ({})'.format(", ".join(operations.keys())), default='prepare-indices')
def start_process(operation, host):
  if operation in operations.keys():
    operations[operation](host)

if __name__ == '__main__':
  start_process()
