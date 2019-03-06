from operations.indices import ClickhouseIndices
from operations.internal_transactions import ClickhouseInternalTransactions
from operations.blocks import ClickhouseBlocks
from operations.contract_transactions import ClickhouseContractTransactions
from operations.contracts import ClickhouseContracts
from operations.inputs import ClickhouseTransactionsInputs, ClickhouseEventsInputs
from operations.events import ClickhouseEvents
from operations.token_holders import ClickhouseTokenHolders
from operations.token_prices import ClickhouseTokenPrices
from operations.contract_methods import ClickhouseContractMethods
from time import sleep
import os


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


def extract_events():
    print("Extracting events...")
    events = ClickhouseEvents()
    events.extract_events()


def parse_transactions_inputs():
    print("Parsing transactions inputs...")
    contracts = ClickhouseTransactionsInputs()
    contracts.decode_inputs()


def parse_events_inputs():
    print("Parsing events inputs...")
    contracts = ClickhouseEventsInputs()
    contracts.decode_inputs()


def extract_token_transactions():
    print("Preparing token transactions view...")
    contracts = ClickhouseTokenHolders()
    contracts.extract_token_transactions()


def extract_prices():
    print("Extracting prices...")
    prices = ClickhouseTokenPrices()
    prices.get_prices_within_interval()


def extract_tokens():
    print("Extracting tokens...")
    tokens = ClickhouseContractMethods()
    tokens.search_methods()


def prepare_indices_and_views():
    prepare_indices()
    prepare_contracts_view()
    extract_token_transactions()


def _fill_database():
    prepare_blocks()
    extract_traces()
    extract_events()
    extract_tokens()


def synchronize():
    while True:
        _fill_database()
        sleep(10)


def synchronize_full():
    while True:
        _fill_database()
        extract_contracts_abi()
        parse_transactions_inputs()
        parse_events_inputs()


def run_tests():
    os.system("nosetests --nologcapture .")
