import unittest 
from contract_transactions import ContractTransactions
from pyelasticsearch import ElasticSearch
from time import sleep
from tqdm import *
from test_utils import TestElasticSearch
from unittest.mock import MagicMock, Mock, call

class ContractTransactionsTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_fast_index(TEST_TRANSACTIONS_INDEX)
    self.client.recreate_index(TEST_CONTRACTS_INDEX)
    self.contract_transactions = ContractTransactions({"contract": TEST_CONTRACTS_INDEX, "transaction": TEST_TRANSACTIONS_INDEX})

  def test_iterate_contract_transactions(self):
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'creates': TEST_TRANSACTION_TO, 'to': None}, id=1, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to': None}, id=2, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to': TEST_TRANSACTION_TO}, id=3, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'nottx', {'creates': TEST_TRANSACTION_TO, 'to': None}, id=4, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'creates': TEST_TRANSACTION_TO, 'to': None, 'to_contract': True}, id=5, refresh=True)
    iterator = self.contract_transactions._iterate_contract_transactions()
    transactions = next(iterator)
    transactions = [transaction['_id'] for transaction in transactions]
    self.assertCountEqual(['1'], transactions)

  def test_extract_contract_from_transaction(self):
    transaction = {
      "hash": "0x0",
      "creates": "0x1",
      "from": "0x2",
      "input": "0x3",
      "blockNumber": 100
    }
    contract = self.contract_transactions._extract_contract_from_transactions(transaction)
    assert contract["owner"] == transaction["from"]
    assert contract["address"] == transaction["creates"]
    assert contract["id"] == transaction["creates"]
    assert contract["parent_transaction"] == transaction["hash"]
    assert contract["block_number"] == transaction["blockNumber"]
    assert contract["bytecode"] == transaction["input"]

  def test_extract_contract_addresses(self):
    transactions_list = [
      [{"_source": "transaction" + str(i)} for i in range(10)],
      [{"_source": "transaction" + str(i)} for i in range(10, 11)]
    ]
    self.contract_transactions._iterate_contract_transactions = MagicMock(return_value=transactions_list)
    self.contract_transactions._extract_contract_from_transactions = MagicMock(return_value="contract")
    self.contract_transactions.client.bulk_index = MagicMock()

    process = Mock()
    process.configure_mock(
      iterate=self.contract_transactions._iterate_contract_transactions,
      extract=self.contract_transactions._extract_contract_from_transactions,
      index=self.contract_transactions.client.bulk_index
    )
    calls = [call.iterate()]
    for transactions in transactions_list:
      for transaction in transactions:
        calls.append(call.extract(transaction["_source"]))
      calls.append(call.index(
        refresh=True,
        doc_type='contract',
        index=TEST_CONTRACTS_INDEX,
        docs=["contract" for _ in transactions]
      ))

    self.contract_transactions._extract_contract_addresses()

    process.assert_has_calls(calls)

  def test_iterate_contracts(self):
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_TRANSACTION_TO}, id=1, refresh=True)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_TRANSACTION_TO_CONTRACT}, id=2, refresh=True)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_TRANSACTION_TO_CONTRACT, 'transactions_detected': True}, id=3, refresh=True)
    iterator = self.contract_transactions._iterate_contracts()
    contracts = [c for contracts_list in iterator for c in contracts_list]
    contracts = [contract['_id'] for contract in contracts]
    self.assertCountEqual(["1", "2"], contracts)

  def test_detect_transactions_by_big_portion_of_contracts(self):
    contract_transactions = [{'creates': TEST_TRANSACTION_TO + str(i), 'to': None, 'id': i + 1} for i in range(1000)]
    self.client.bulk_index(docs=contract_transactions, doc_type='tx', index=TEST_TRANSACTIONS_INDEX, refresh=True)
    transactions = [{'to': TEST_TRANSACTION_TO + str(i), 'id': i + 1} for i in range(1000)]
    self.client.bulk_index(docs=transactions, doc_type='tx', index=TEST_TRANSACTIONS_INDEX, refresh=True)
    self.contract_transactions._detect_transactions_by_contracts([TEST_TRANSACTION_TO + str(i) for i in range(1000)])
    transactions = self.client.search("to_contract:true", index=TEST_TRANSACTIONS_INDEX, doc_type="tx", size=1000)['hits']['hits']
    assert len(transactions) == 1000

  def test_set_flag_for_processed_contracts(self):
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_TRANSACTION_TO}, id=1, refresh=True)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_TRANSACTION_TO_CONTRACT}, id=2, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'creates': TEST_TRANSACTION_TO, 'to': None}, id=1, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'creates': TEST_TRANSACTION_TO, 'to': None}, id=2, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'creates': TEST_TRANSACTION_TO, 'to': None}, id=3, refresh=True)
    self.contract_transactions._detect_transactions_by_contracts([TEST_TRANSACTION_TO])
    contract = self.client.get(index=TEST_CONTRACTS_INDEX, doc_type="contract", id=1)["_source"]
    assert contract["transactions_detected"]

  def test_detect_contract_transactions(self):
    contracts_list = [[TEST_TRANSACTION_TO + str(j*10 + i) for i in range(10)] for j in range(5)]
    contracts_from_es_list = [[{"_source": {"address": contract}} for contract in contracts] for contracts in contracts_list]
    self.contract_transactions._extract_contract_addresses = MagicMock()
    self.contract_transactions._iterate_contracts = MagicMock(return_value=contracts_from_es_list)
    self.contract_transactions._detect_transactions_by_contracts = MagicMock()
    process = Mock()
    process.configure_mock(
      extract=self.contract_transactions._extract_contract_addresses,
      iterate=self.contract_transactions._iterate_contracts,
      detect=self.contract_transactions._detect_transactions_by_contracts
    )

    self.contract_transactions.detect_contract_transactions()

    process.assert_has_calls([
      call.extract(),
      call.iterate()
    ] + [call.detect(contracts) for contracts in contracts_list])

TEST_TRANSACTIONS_INDEX = 'test-ethereum-transactions'
TEST_CONTRACTS_INDEX = 'test-ethereum-contracts'
TEST_TRANSACTION_INPUT = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_TO = '0xb1631db29e09ec5581a0ec398f1229abaf105d3524c49727621841af947bdc44'
TEST_TRANSACTION_TO_COMMON = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_TO_CONTRACT = '0x69a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'