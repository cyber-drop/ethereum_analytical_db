from operations.indices import ElasticSearchIndices

def prepare_indices():
  print("Preparing indices...")
  indices = ElasticSearchIndices()
  indices.prepare_indices()

# def prepare_blocks(host):
#   print("Preparing blocks...")
#   blocks = Blocks(INDICES, host)
#   blocks.create_blocks()
#
# def detect_contracts(host):
#   print("Detecting contracts...")
#   contract_transactions = ContractTransactions(INDICES, host)
#   contract_transactions.extract_contract_addresses()
#
# def detect_contract_transactions(host):
#   print("Detecting transactions to contracts...")
#   contract_transactions = ContractTransactions(INDICES, host)
#   contract_transactions.detect_contract_transactions()
#
# def extract_traces(host):
#   print("Extracting traces...")
#   internal_transactions = InternalTransactions(INDICES, host)
#   internal_transactions.extract_traces()
#
# def extract_contracts_abi(host):
#   print("Extracting ABIs...")
#   contracts = Contracts(INDICES, host)
#   contracts.save_contracts_abi()
#
# def parse_inputs(host):
#   print("Parsing inputs...")
#   internal_transactions = Contracts(INDICES, host)
#   internal_transactions.decode_inputs()
#
# def search_methods(host):
#   print("Searching for tokens...")
#   contract_methods = ContractMethods(INDICES, host)
#   contract_methods.search_methods()
#
# def extract_token_transactions(host):
#   print("Extracting token transactions...")
#   token_holders = InternalTokenTransactions(INDICES, host)
#   token_holders.get_listed_tokens_txs()
#
# def extract_prices(host):
#   print("Extracting token prices...")
#   token_prices = TokenPrices(INDICES, host)
#   token_prices.get_prices_within_interval()
#
# def extract_transactions_prices(host, currency):
#   print("Extracting transactions {} value...".format(currency))
#   transactions_prices = TokenTransactionsPrices()
#   transactions_prices.extract_transactions_prices(currency)
#
# def extract_transactions_prices_usd(host):
#   extract_transactions_prices(host, 'USD')
#
# def extract_transactions_prices_eth(host):
#   extract_transactions_prices(host, 'ETH')
#
# def extract_transaction_fees(host):
#   transaction_fees = TransactionFees()
#   transaction_fees.extract_transaction_fees()
#
# def run_loop(host):
#   print("Running loop...")
#   while True:
#     prepare_blocks(host)
#     extract_traces(host)
#     detect_contracts(host)
#     detect_contract_transactions(host)
#     extract_contracts_abi(host)
#     search_methods(host)
#     parse_inputs(host)
#     extract_prices(host)
#     extract_token_transactions(host)
#     extract_transactions_prices(host, 'USD')
#     extract_transactions_prices(host, 'ETH')
#     extract_transaction_fees()
#     sleep(10)
