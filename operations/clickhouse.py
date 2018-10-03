from operations.indices import ClickhouseIndices
from operations.internal_transactions import ClickhouseInternalTransactions
from operations.blocks import ClickhouseBlocks

def prepare_indices():
  print("Preparing indices...")
  indices = ClickhouseIndices()
  indices.prepare_indices()

def prepare_blocks():
  print("Preparing blocks...")
  blocks = ClickhouseBlocks()
  blocks.create_blocks()

def extract_traces():
  print("Extracting internal transactions...")
  internal_transactions = ClickhouseInternalTransactions()
  internal_transactions.extract_traces()
