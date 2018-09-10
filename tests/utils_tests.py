import unittest
from utils import get_max_block, split_on_chunks, ContractTransactionsIterator
from tests.test_utils import TestElasticSearch, CustomElasticSearch
import config
from unittest.mock import MagicMock

class UtilsTestCase(unittest.TestCase):
  def _create_contracts_iterator(self):
    self.contracts_iterator = ContractTransactionsIterator()
    self.contracts_iterator.client = CustomElasticSearch("http://localhost:9200")
    self.contracts_iterator.indices = {
      "contract": TEST_CONTRACTS_INDEX,
      "transaction": TEST_TRANSACTIONS_INDEX
    }
    self.contracts_iterator.doc_type = "tx"
    self.contracts_iterator.block_prefix = "test"
    self.contracts_iterator.index = "transaction"

  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_BLOCKS_INDEX)
    self.client.recreate_index(TEST_CONTRACTS_INDEX)
    self._create_contracts_iterator()

  def tearDown(self):
    config.PROCESSED_CONTRACTS.clear()

  def test_create_transactions_request(self):
    test_max_block = 40
    test_contracts = [
      {"_source": {"address": "0x1", "tx_test_block": 10}},
      {"_source": {"address": "0x2", "tx_test_block": 30}},
    ]
    transactions_request = self.contracts_iterator._create_transactions_request(
      test_contracts,
      test_max_block
    )
    self.assertCountEqual([
      {"bool": {"must": [
        {"terms": {"to": ["0x1"]}},
        {"range": {"blockNumber": {"gt": 10, "lte": 40}}}
      ]}},
      {"bool": {"must": [
        {"terms": {"to": ["0x2"]}},
        {"range": {"blockNumber": {"gt": 30, "lte": 40}}}
      ]}},
    ], transactions_request["bool"]["should"])

  def test_create_transactions_request_empty_block(self):
    test_max_block = 40
    test_contracts = [
      {"_source": {"address": "0x1"}},
    ]
    transactions_request = self.contracts_iterator._create_transactions_request(
      test_contracts,
      test_max_block
    )
    self.assertCountEqual(["0x1"], transactions_request["bool"]["should"][0]["bool"]["must"][0]["terms"]["to"])

  def test_create_transactions_request_multiple_blocks(self):
    test_max_block = 40
    test_contracts = [
      {"_source": {"address": "0x1", "tx_test_block": 10}},
      {"_source": {"address": "0x2", "tx_test_block": 10}},
    ]
    transactions_request = self.contracts_iterator._create_transactions_request(
      test_contracts,
      test_max_block
    )
    self.assertCountEqual(["0x1", "0x2"], transactions_request["bool"]["should"][0]["bool"]["must"][0]["terms"]["to"])

  def test_save_inputs_decoded(self):
    test_max_block = 100
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': "0x1"}, id=1, refresh=True)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': "0x2"}, id=2, refresh=True)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': "0x3"}, id=3, refresh=True)
    self.contracts_iterator._save_max_block(["0x1", "0x3"], test_max_block)

    contracts = self.client.search(index=TEST_CONTRACTS_INDEX, doc_type='contract', query="tx_test_block:" + str(test_max_block))['hits']['hits']
    contracts = [contract["_source"]["address"] for contract in contracts]
    self.assertCountEqual(contracts, ["0x1", "0x3"])

  def test_iterate_contracts(self):
    test_max_block = 2
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {
      "tx_test_block": 1,
      "address": "0x1",
      "test": True,
      "blockNumber": 1,
    }, refresh=True, id=1)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {
      "tx_test_block": 2,
      "address": "0x2",
      "test": True,
      "blockNumber": 1
    }, refresh=True, id=2)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {
      "tx_test_block": 3,
      "address": "0x3",
      "test": True,
      "blockNumber": 1
    }, refresh=True, id=3)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {
      "address": "0x4",
      "test": True,
      "blockNumber": 1
    }, refresh=True, id=4)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {
      "address": "0x4",
      "blockNumber": 1
    }, refresh=True, id=5)
    contracts = [
      c["_id"] for contracts in self.contracts_iterator._iterate_contracts(test_max_block, {"term": {"test": True}})
      for c in contracts
    ]
    self.assertCountEqual(contracts, ['1', '4'])

  def test_iterate_contracts_from_list(self):
    config.PROCESSED_CONTRACTS.append("0x1")
    test_max_block = 2
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {
      "tx_test_block": 1,
      "address": "0x1",
      "blockNumber": 1,
    }, refresh=True, id=1)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {
      "tx_test_block": 1,
      "address": "0x2",
      "blockNumber": 1
    }, refresh=True, id=2)
    contracts = [
      c["_id"] for contracts in self.contracts_iterator._iterate_contracts(test_max_block, {"query_string": {"query": "*"}})
      for c in contracts
    ]
    self.assertCountEqual(contracts, ['1'])

  def test_iterate_contracts_without_specified_block(self):
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {
      "tx_test_block": 1,
      "address": "0x1",
      "blockNumber": 1,
    }, refresh=True, id=1)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {
      "address": "0x2",
      "blockNumber": 1,
    }, refresh=True, id=2)

    contracts_without_max_block = [
      c["_id"] for contracts in self.contracts_iterator._iterate_contracts(partial_query={"query_string": {"query": "*"}})
      for c in contracts
    ]
    contracts_with_zero_max_block = [
      c["_id"] for contracts in
      self.contracts_iterator._iterate_contracts(0, {"query_string": {"query": "*"}})
      for c in contracts
    ]

    self.assertCountEqual(contracts_without_max_block, ['1', '2'])
    self.assertCountEqual(contracts_with_zero_max_block, ['2'])

  def test_iterate_transactions_by_query(self):
    self.contracts_iterator._create_transactions_request = MagicMock(return_value={
      "query_string": {"query": "*"}
    })
    self.client.index(TEST_TRANSACTIONS_INDEX, "tx", {
      'to': "0x1",
      "test": True,
    }, id=1, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, "tx", {
      'to': "0x1",
      "test": False,
    }, id=2, refresh=True)
    targets = [{"_source": {"address": "0x1"}}]
    transactions = self.contracts_iterator._iterate_transactions(targets, 0, {
      "term": {
        "test": True
      }
    })
    transactions = [t["_id"] for transactions_list in transactions for t in transactions_list]
    self.assertCountEqual(transactions, ['1'])

  def test_iterate_transactions_by_targets_select_unprocessed_transactions(self):
    self.client.index(TEST_TRANSACTIONS_INDEX, "tx", {
      'to': "0x1",
      "blockNumber": 1
    }, id=1, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, "tx", {
      'to': "0x1",
      "blockNumber": 2
    }, id=2, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, "tx", {
      'to': "0x1",
      "blockNumber": 3
    }, id=3, refresh=True)
    targets = [{"_source": {"address": "0x1", "tx_test_block": 1}}]
    test_query = {
      "query_string": {"query": "*"}
    }
    transactions = [c for c in self.contracts_iterator._iterate_transactions(targets, 2, test_query)]
    transactions = [t["_id"] for transactions_list in transactions for t in transactions_list]
    print(transactions)
    self.assertCountEqual(transactions, ['2'])

  def test_split_on_chunks(self):
    test_list = list(range(10))
    test_chunks = list(split_on_chunks(test_list, 3))
    self.assertSequenceEqual(test_chunks, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])

  def test_get_max_block(self):
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 0
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 1
    }, refresh=True)
    max_block = get_max_block()
    assert max_block == 1
    assert type(max_block) == int

  def test_get_max_block_by_a_query(self):
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 0,
      "trace": True
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 1
    }, refresh=True)
    max_block = get_max_block("trace:true")
    assert max_block == 0

  def test_get_max_block_in_empty_index(self):
    max_block = get_max_block("*", 1)
    assert max_block == 1

  def xtest_get_max_consistent_block(self):
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 0,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 1,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 2
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 4
    }, refresh=True)
    max_block = get_max_block()
    assert max_block == 2

  def xtest_get_max_consistent_block_return_min_consistent_block_if_ended(self):
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 0,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 1,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 3
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 5
    }, refresh=True)
    max_block = get_max_block(min_consistent_block=3)
    assert max_block == 3

  def xtest_get_max_consistent_block_ignore_inconsistency_before_min_block(self):
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 0,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 2,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 3
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 5
    }, refresh=True)
    max_block = get_max_block(min_consistent_block=2)
    assert max_block == 3

TEST_BLOCKS_INDEX = "test-ethereum-blocks"
TEST_CONTRACTS_INDEX = "test-ethereum-contract"
TEST_TRANSACTIONS_INDEX = "test-ethereum-transaction"
config.INDICES.update({
  "block": TEST_BLOCKS_INDEX
})