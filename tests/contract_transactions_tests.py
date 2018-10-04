import unittest 
from operations.contract_transactions import ElasticSearchContractTransactions, ClickhouseContractTransactions
from pyelasticsearch import ElasticSearch
from time import sleep
from tqdm import *
from tests.test_utils import TestElasticSearch, TestClickhouse
from unittest.mock import MagicMock, Mock, call, ANY, patch
from operations.indices import ClickhouseIndices

class ElasticSearchContractTransactionsTestCase(unittest.TestCase):
  contract_transactions_class = ElasticSearchContractTransactions
  index = "internal_transaction"
  doc_type = "itx"

  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_fast_index(TEST_TRANSACTIONS_INDEX)
    self.client.recreate_index(TEST_CONTRACTS_INDEX)
    self.contract_transactions = self.contract_transactions_class({"contract": TEST_CONTRACTS_INDEX, self.index: TEST_TRANSACTIONS_INDEX})

  def test_iterate_contract_transactions(self):
    """
    Test iterations through transactions that create contracts
    """
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
    """
    Test extracting contract from a defined transaction
    """
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

  def test_extract_contract_addresses(self):
    """
    Test extracting contracts from transactions to ElasticSearch
    """
    transactions_list = [
      [{"_source": {"hash": "transaction" + str(i)}} for i in range(10)],
      [{"_source": {"hash": "transaction" + str(i)}} for i in range(10, 11)]
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
    """
    Test save flag for processed transactions
    """
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

  def test_iterate_contracts(self):
    """
    Test iterations through all contracts
    """
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_TRANSACTION_TO}, id=1, refresh=True)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_TRANSACTION_TO_CONTRACT}, id=2, refresh=True)
    iterator = self.contract_transactions._iterate_contracts_without_detected_transactions(0)
    contracts = [c for contracts_list in iterator for c in contracts_list]
    contracts = [contract['_id'] for contract in contracts]
    self.assertCountEqual(["1", "2"], contracts)

  def test_iterate_unprocessed_contracts(self):
    """
    Test iterations through unprocessed contracts with helper class usage
    """
    test_iterator = "iterator"
    test_max_block = 0
    self.contract_transactions._iterate_contracts = MagicMock(return_value=test_iterator)

    contracts = self.contract_transactions._iterate_contracts_without_detected_transactions(test_max_block)
    self.contract_transactions._iterate_contracts.assert_any_call(test_max_block, ANY)
    assert contracts == test_iterator

  def test_detect_transactions_by_contracts(self):
    """
    Test to_contract flag placement in ElasticSearch
    """
    test_query = {"test": "query"}
    test_max_block = 0
    self.contract_transactions.client.update_by_query = MagicMock()
    self.contract_transactions._create_transactions_request = MagicMock(return_value=test_query)
    contracts = [{"_source": {"address": "0x1"}}, {"_source": {"address": "0x2"}}]
    contracts_addresses = ["0x1", "0x2"]
    self.contract_transactions._detect_transactions_by_contracts(contracts, test_max_block)

    self.contract_transactions._create_transactions_request.assert_any_call(ANY, test_max_block)
    self.contract_transactions.client.update_by_query.assert_any_call(
      TEST_TRANSACTIONS_INDEX,
      self.doc_type,
      {
        "bool": {
          "must": [
            {"terms": {"to": contracts_addresses}},
            test_query
          ]
        }
      },
      "ctx._source.to_contract = true"
    )

  def test_detect_contract_transactions(self):
    """
    Test contract transactions detection process
    """
    test_max_block = 10
    contracts_list = [[TEST_TRANSACTION_TO + str(j * 10 + i) for i in range(10)] for j in range(5)]
    contracts_from_es_list = [[{"_source": {"address": contract}} for contract in contracts] for contracts in
                              contracts_list]
    self.contract_transactions.extract_contract_addresses = MagicMock()
    self.contract_transactions._iterate_contracts_without_detected_transactions = MagicMock(return_value=contracts_from_es_list)
    self.contract_transactions._detect_transactions_by_contracts = MagicMock()
    self.contract_transactions._save_max_block = MagicMock()
    test_max_block_mock = MagicMock(side_effect=[test_max_block])
    with patch('utils.get_max_block', test_max_block_mock):
      process = Mock()
      process.configure_mock(
        get_max_block=test_max_block_mock,
        iterate=self.contract_transactions._iterate_contracts_without_detected_transactions,
        detect=self.contract_transactions._detect_transactions_by_contracts,
        save=self.contract_transactions._save_max_block
      )

      self.contract_transactions.detect_contract_transactions()

      call_part = []
      for index, contracts in enumerate(contracts_from_es_list):
        call_part.append(call.detect(contracts, test_max_block))
        call_part.append(call.save(contracts_list[index], test_max_block))
      process.assert_has_calls([
                                 call.get_max_block(),
                                 call.iterate(test_max_block)
                               ] + call_part)

class ClickhouseContractTransactionsTestCase(unittest.TestCase):
  def setUp(self):
    self.indices = {
      "internal_transaction": TEST_TRANSACTIONS_INDEX,
      "contract": TEST_CONTRACTS_INDEX
    }
    self.client = TestClickhouse()
    for index in self.indices.values():
      self.client.send_sql_request("DROP TABLE IF EXISTS {}".format(index))
    ClickhouseIndices(self.indices).prepare_indices()
    self.contract_transactions = ClickhouseContractTransactions(self.indices)
    self.contract_transactions.extract_contract_addresses()

  def test_extract_contract_addresses(self):
    transaction = {
      "id": "0x12345",
      "type": "create",
      "address": "0x0",
      "blockNumber": 1000,
      "from": "0x01",
      "code": "0x12345678"
    }
    self.client.bulk_index(index=TEST_TRANSACTIONS_INDEX, docs=[transaction])
    result = self.client.search(index=TEST_CONTRACTS_INDEX, fields=[
      "address",
      "blockNumber",
      "owner",
      "bytecode"
    ])
    contract = result[0]
    print(contract)
    assert contract["_id"] == transaction["id"]
    assert contract['_source']["address"] == transaction["address"]
    assert contract['_source']["blockNumber"] == transaction["blockNumber"]
    assert contract['_source']["owner"] == transaction["from"]
    assert contract['_source']["bytecode"] == transaction["code"]

  def test_extract_contract_addresses_if_exists(self):
    self.contract_transactions.extract_contract_addresses()

  def test_extract_contract_addresses_ignore_transactions(self):
    transactions = [{
      "id": 1,
      "type": "call"
    }, {
      "id": 2,
      "type": "create",
      "address": "0x0",
      "error": "Out of gas"
    }, {
      "id": 3,
      "type": "create",
      "address": "0x0",
      "parent_error": True,
    }]
    self.client.bulk_index(index=TEST_TRANSACTIONS_INDEX, docs=transactions)
    count = self.client.count(index=TEST_CONTRACTS_INDEX)
    assert not count

  def test_extract_contract_addresses_ignore_duplicates(self):
    transaction = {
      "id": 1,
      "type": "create"
    }
    self.client.bulk_index(index=TEST_TRANSACTIONS_INDEX, docs=[transaction, transaction])
    count = self.client.count(index=TEST_CONTRACTS_INDEX)
    assert count == 1

  # Cases:
  # self.client.index(TEST_TRANSACTIONS_INDEX, 'itx', {'type': "call"}, id=1, refresh=True)
  # self.client.index(TEST_TRANSACTIONS_INDEX, 'itx', {'type': "create"}, id=2, refresh=True)
  # self.client.index(TEST_TRANSACTIONS_INDEX, 'itx', {'type': "create", "error": "Out of gas"}, id=3, refresh=True)
  # self.client.index(TEST_TRANSACTIONS_INDEX, 'nottx', {'type': "create"}, id=4, refresh=True)
  # self.client.index(TEST_TRANSACTIONS_INDEX, 'itx', {'type': "create", "contract_created": True}, id=5, refresh=True)

  # Fields:
  # assert contract["owner"] == transaction["from"]
  # assert contract["blockNumber"] == transaction["blockNumber"]
  # assert contract["parent_transaction"] == transaction_id
  # assert contract["address"] == transaction["address"]
  # assert contract["id"] == transaction["address"]
  # assert contract["bytecode"] == transaction["code"]
  pass

TEST_TRANSACTIONS_INDEX = 'test_ethereum_transactions'
TEST_CONTRACTS_INDEX = 'test_ethereum_contracts'
TEST_TRANSACTION_INPUT = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_TO = '0xb1631db29e09ec5581a0ec398f1229abaf105d3524c49727621841af947bdc44'
TEST_TRANSACTION_TO_COMMON = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_TO_CONTRACT = '0x69a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'