from operations.indices import ClickhouseIndices
from operations.internal_transactions import ClickhouseInternalTransactions
from operations.blocks import ClickhouseBlocks
from operations.contract_transactions import ClickhouseContractTransactions
from operations.contracts import ClickhouseContracts

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
