import unittest 
from contract_transactions import ExternalContractTransactions, InternalContractTransactions
from pyelasticsearch import ElasticSearch
from time import sleep
from tqdm import *
from test_utils import TestElasticSearch
from unittest.mock import MagicMock, Mock, call

class ContractTransactionsTestCase():
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_fast_index(TEST_TRANSACTIONS_INDEX)
    self.client.recreate_index(TEST_CONTRACTS_INDEX)
    self.contract_transactions = self.contract_transactions_class({"contract": TEST_CONTRACTS_INDEX, self.index: TEST_TRANSACTIONS_INDEX})

  def test_extract_contract_addresses(self):
    transactions_list = [
      [{"_source": "transaction" + str(i)} for i in range(10)],
      [{"_source": "transaction" + str(i)} for i in range(10, 11)]
    ]
    self.contract_transactions._iterate_contract_transactions = MagicMock(return_value=transactions_list)
    self.contract_transactions._extract_contract_from_transactions = MagicMock(return_value="contract")
    self.contract_transactions.client.bulk_index = MagicMock()
    self.contract_transactions._save_contract_created = MagicMock()

    process = Mock()
    process.configure_mock(
      iterate=self.contract_transactions._iterate_contract_transactions,
      extract=self.contract_transactions._extract_contract_from_transactions,
      save_flag=self.contract_transactions._save_contract_created,
      index=self.contract_transactions.client.bulk_index
    )
    calls = [call.iterate()]
    for transactions in transactions_list:
      for transaction in transactions:
        calls.append(call.extract(transaction))
      calls.append(call.index(
        refresh=True,
        doc_type='contract',
        index=TEST_CONTRACTS_INDEX,
        docs=["contract" for _ in transactions]
      ))
      calls.append(call.save_flag(transactions))
    self.contract_transactions.extract_contract_addresses()

    process.assert_has_calls(calls)

  def test_save_flag_for_contracts(self):
    transactions = [{
      "hash": "0x" + str(i)
    } for i in range(10)]
    self.client.bulk_index(
      index=TEST_TRANSACTIONS_INDEX,
      doc_type=self.doc_type,
      docs=transactions,
      refresh=True
    )
    transactions_from_elasticsearch = self.client.search(
      index=TEST_TRANSACTIONS_INDEX,
      doc_type=self.doc_type,
      query="*",
      size=len(transactions)
    )['hits']['hits']

    self.contract_transactions._save_contract_created(transactions_from_elasticsearch)
    transactions_count = self.client.count(
      index=TEST_TRANSACTIONS_INDEX,
      doc_type=self.doc_type,
      query="_exists_:contract_created"
    )["count"]
    assert transactions_count == 10

class InternalContractTransactionsTestCase(ContractTransactionsTestCase, unittest.TestCase):
  contract_transactions_class = InternalContractTransactions
  index = "internal_transaction"
  doc_type = "itx"

  def test_iterate_internal_contract_transactions(self):
    self.client.index(TEST_TRANSACTIONS_INDEX, 'itx', {'type': "call"}, id=1, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'itx', {'type': "create"}, id=2, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'itx', {'type': "create", "error": "Out of gas"}, id=3, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'nottx', {'type': "create"}, id=4, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'itx', {'type': "create", "contract_created": True}, id=5, refresh=True)
    iterator = self.contract_transactions._iterate_contract_transactions()
    transactions = next(iterator)
    transactions = [transaction['_id'] for transaction in transactions]
    self.assertCountEqual(['2'], transactions)

  def test_extract_contract_from_internal_transaction(self):
    transaction = {
      "from": "0x0",
      "input": "0x1",
      "address": "0x2",
      "code": "0x3",
      "blockNumber": 100
    }
    transaction_id = "0x10"
    contract = self.contract_transactions._extract_contract_from_transactions({
      "_source": transaction,
      "_id": transaction_id
    })
    assert contract["owner"] == transaction["from"]
    assert contract["blockNumber"] == transaction["blockNumber"]
    assert contract["parent_transaction"] == transaction_id
    assert contract["address"] == transaction["address"]
    assert contract["id"] == transaction["address"]
    assert contract["bytecode"] == transaction["code"]

class ExternalContractTransactionsTestCase(ContractTransactionsTestCase, unittest.TestCase):
  contract_transactions_class = ExternalContractTransactions
  index = "transaction"
  doc_type = "tx"

  def test_extract_contract_from_transaction(self):
    transaction = {
      "hash": "0x0",
      "creates": "0x1",
      "from": "0x2",
      "input": "0x3",
      "blockNumber": 100
    }
    transaction_id = "0x10"
    contract = self.contract_transactions._extract_contract_from_transactions({
      "_source": transaction,
      "_id": transaction_id
    })
    assert contract["owner"] == transaction["from"]
    assert contract["address"] == transaction["creates"]
    assert contract["id"] == transaction["creates"]
    assert contract["parent_transaction"] == transaction_id
    assert contract["blockNumber"] == transaction["blockNumber"]
    assert contract["bytecode"] == transaction["input"]

  def test_iterate_contract_transactions(self):
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'creates': TEST_TRANSACTION_TO, 'to': None}, id=1, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to': None}, id=2, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to': TEST_TRANSACTION_TO}, id=3, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'creates': TEST_TRANSACTION_TO, 'to': None, 'error': "Out of gas"}, id=4, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'nottx', {'creates': TEST_TRANSACTION_TO, 'to': None}, id=5, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'creates': TEST_TRANSACTION_TO, 'to': None, "contract_created": True}, id=6, refresh=True)
    iterator = self.contract_transactions._iterate_contract_transactions()
    transactions = next(iterator)
    transactions = [transaction['_id'] for transaction in transactions]
    self.assertCountEqual(['1'], transactions)

TEST_TRANSACTIONS_INDEX = 'test-ethereum-transactions'
TEST_CONTRACTS_INDEX = 'test-ethereum-contracts'
TEST_TRANSACTION_INPUT = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_TO = '0xb1631db29e09ec5581a0ec398f1229abaf105d3524c49727621841af947bdc44'
TEST_TRANSACTION_TO_COMMON = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_TO_CONTRACT = '0x69a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'