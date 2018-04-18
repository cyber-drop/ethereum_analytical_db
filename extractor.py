import click
from contract_transactions import ContractTransactions
from internal_transactions import InternalTransactions
from contracts import Contracts

@click.command()
@click.option('--index', help='Elasticsearch index name', default='ethereum-transaction')
@click.option('--operation', help='Action to perform (detect-contracts, extract-traces, parse-inputs)', default='detect-contracts')
def start_process(index, operation):
  contract_transactions = ContractTransactions(index)
  internal_transactions = InternalTransactions(index)
  contracts = Contracts(index)
  if operation == "detect-contracts":
    contract_transactions.detect_contract_transactions()
  elif operation == "extract-traces":
    internal_transactions.extract_traces()
  elif operation == "parse-inputs":
    contracts.decode_inputs()


if __name__ == '__main__':
  start_process()
