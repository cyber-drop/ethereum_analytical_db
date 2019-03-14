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
    """
    Prepare tables in database
    """
    print("Preparing indices...")
    indices = ClickhouseIndices()
    indices.prepare_indices()


def prepare_blocks():
    """
    Extract blocks with timestamps
    """
    print("Preparing blocks...")
    blocks = ClickhouseBlocks()
    blocks.create_blocks()


def prepare_contracts_view():
    """
    Prepare material view with contracts extracted from transactions table
    """
    print("Preparing contracts view...")
    contract_transactions = ClickhouseContractTransactions()
    contract_transactions.extract_contract_addresses()


def extract_traces():
    """
    Extract internal transactions
    """
    print("Extracting internal transactions...")
    internal_transactions = ClickhouseInternalTransactions()
    internal_transactions.extract_traces()


def extract_contracts_abi():
    """
    Extract ABI description from etherscan.io

    Works only for contracts specified in config
    """
    print("Extracting ABIs...")
    contracts = ClickhouseContracts()
    contracts.save_contracts_abi()


def extract_events():
    """
    Extract events
    """
    print("Extracting events...")
    events = ClickhouseEvents()
    events.extract_events()


def parse_transactions_inputs():
    """
    Start input parsing for transactions.

    The operation works only for contracts specified in config.
    """
    print("Parsing transactions inputs...")
    contracts = ClickhouseTransactionsInputs()
    contracts.decode_inputs()


def parse_events_inputs():
    """
    Start input parsing for events.

    The operation works only for contracts specified in config
    """
    print("Parsing events inputs...")
    contracts = ClickhouseEventsInputs()
    contracts.decode_inputs()


def extract_token_transactions():
    """
    Prepare material view with erc20 transactions
    extracted from transactions table.
    """
    print("Preparing token transactions view...")
    contracts = ClickhouseTokenHolders()
    contracts.extract_token_transactions()


def extract_prices():
    """
    Download exchange rates

    Will extract token capitalization, ETH, BTC and USD prices
    from cryptocompare.com and coinmarketcap.com
    """
    print("Extracting prices...")
    prices = ClickhouseTokenPrices()
    prices.get_prices_within_interval()


def extract_tokens():
    """
    Extract ERC20 token names, symbols, total supply and etc.
    """
    print("Extracting tokens...")
    tokens = ClickhouseContractMethods()
    tokens.search_methods()


def prepare_indices_and_views():
    """
    Prepare all indices and views in database
    """
    prepare_indices()
    prepare_contracts_view()
    extract_token_transactions()


def _fill_database():
    prepare_blocks()
    extract_traces()
    extract_events()
    extract_tokens()


def synchronize():
    """
    Run partial synchronization of the database.

    Will extract only new blocks, internal transactions, events and token descriptions
    """
    while True:
        _fill_database()
        sleep(10)


def synchronize_full():
    """
    Run full synchronization of the database
    """
    while True:
        _fill_database()
        extract_contracts_abi()
        parse_transactions_inputs()
        parse_events_inputs()
        extract_prices()
        sleep(10)
        

def run_tests():
    """
    Run tests
    """
    os.system("nosetests --nologcapture .")
