from operations.indices import ClickhouseIndices
from operations.internal_transactions import ClickhouseInternalTransactions
from operations.blocks import ClickhouseBlocks
from operations.contract_transactions import ClickhouseContractTransactions
from operations.contracts import ClickhouseContracts
from operations.transaction_fees import ClickhouseTransactionFees
from operations.events import Events

def prepare_indices():
  print("Preparing indices...")
  indices = ClickhouseIndices()
  indices.prepare_indices()

def prepare_blocks():
  print("Preparing blocks...")
  blocks = ClickhouseBlocks()
  blocks.create_blocks()

def prepare_contracts_view():
  print("Preparing contracts view...")
  contract_transactions = ClickhouseContractTransactions()
  contract_transactions.extract_contract_addresses()

def extract_traces():
  print("Extracting internal transactions...")
  internal_transactions = ClickhouseInternalTransactions()
  internal_transactions.extract_traces()

def extract_contracts_abi():
  print("Extracting ABIs...")
  contracts = ClickhouseContracts()
  contracts.save_contracts_abi()

def extract_transaction_fees():
  print("Extracting transaction fees...")
  transaction_fees = ClickhouseTransactionFees()
  transaction_fees.extract_transaction_fees()

def extract_events():
  print("Extracting events...")
  events = Events()
  events.extract_events()

def parse_transactions_inputs():
  print("Parsing transactions inputs...")
  contracts = ClickhouseContracts()
  contracts.decode_inputs()
