import unittest
from utils import split_on_chunks, make_range_query
from utils import ClickhouseContractTransactionsIterator
from tests.test_utils import TestElasticSearch, CustomElasticSearch, TestClickhouse
import config
from unittest.mock import MagicMock, ANY
from clients.custom_clickhouse import CustomClickhouse

class UtilsTestCase(unittest.TestCase):
  def test_split_on_chunks(self):
    test_list = list(range(10))
    test_chunks = list(split_on_chunks(test_list, 3))
    self.assertSequenceEqual(test_chunks, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])

  def test_make_range_query(self):
    assert make_range_query("block", (0, 3)) == "block >= 0 AND block < 3"
    assert make_range_query("block", (None, 3)) == "block < 3"
    assert make_range_query("block", (0, None)) == "block >= 0"
    assert make_range_query("block", (None, None)) == "block IS NOT NULL"

  def test_make_complex_range_query(self):
    assert make_range_query("block", (0, 3), (10, 100)) == "(block >= 0 AND block < 3) OR (block >= 10 AND block < 100)"

class ClickhouseIteratorTestCase(unittest.TestCase):
  client_class = CustomClickhouse
  iterator_class = ClickhouseContractTransactionsIterator

  def setUp(self):
    self.client = TestClickhouse()
    self.indices = {
      "block": TEST_BLOCKS_INDEX,
      "contract": TEST_CONTRACTS_INDEX,
      "internal_transaction": TEST_TRANSACTIONS_INDEX,
      "contract_block": TEST_CONTRACT_BLOCK_INDEX,
      "block_flag": TEST_BLOCK_FLAGS_INDEX
    }
    self.client.prepare_indices(self.indices)
    self._create_contracts_iterator()

  def _create_contracts_iterator(self):
    self.contracts_iterator = self.iterator_class()
    self.contracts_iterator.client = self.client_class()
    self.contracts_iterator.indices = self.indices
    self.contracts_iterator.doc_type = "tx"
    self.contracts_iterator.block_prefix = "test"
    self.contracts_iterator.index = "internal_transaction"

  def tearDown(self):
    config.PROCESSED_CONTRACTS.clear()

  def test_iterate_contracts(self):
    test_max_block = 2
    test_contracts = [{
      "id": 1,
      "address": "0x1",
      "test": True,
      "blockNumber": 1,
    }, {
      "id": 2,
      "address": "0x2",
      "test": True,
      "blockNumber": 1
    }, {
      "id": 3,
      "address": "0x3",
      "test": True,
      "blockNumber": 1
    }, {
      "id": 4,
      "address": "0x4",
      "test": True,
      "blockNumber": 1
    }, {
      "id": 5,
      "address": "0x4",
      "test": False,
      "blockNumber": 1
    }]
    test_contract_blocks = [
      {"id": 1, "name": "tx_test_block", "value": 1},
      {"id": 2, "name": "tx_test_block", "value": 2},
      {"id": 3, "name": "tx_test_block", "value": 3}
    ]
    self.client.bulk_index(index=TEST_CONTRACTS_INDEX, docs=test_contracts)
    self.client.bulk_index(index=TEST_CONTRACT_BLOCK_INDEX, docs=test_contract_blocks)
    contracts = [
      c["_id"] for contracts in self.contracts_iterator._iterate_contracts(test_max_block, "WHERE test = 1")
      for c in contracts
    ]
    self.assertCountEqual(contracts, ['1', '4'])

  def test_iterate_contracts_from_list(self):
    config.PROCESSED_CONTRACTS.append("0x1")
    test_max_block = 2
    test_contracts = [{
      "address": "0x1",
      "blockNumber": 1,
      "id": 1
    }, {
      "address": "0x2",
      "blockNumber": 1,
      "id": 2
    }]
    test_contract_blocks = [
      {"id": 1, "name": "tx_test_block", "value": 1},
      {"id": 2, "name": "tx_test_block", "value": 1},
    ]
    self.client.bulk_index(index=TEST_CONTRACT_BLOCK_INDEX, docs=test_contract_blocks)
    self.client.bulk_index(index=TEST_CONTRACTS_INDEX, docs=test_contracts)
    contracts = [
      c["_id"] for contracts in self.contracts_iterator._iterate_contracts(test_max_block, "WHERE address IS NOT NULL")
      for c in contracts
    ]
    self.assertCountEqual(contracts, ['1'])

  def test_iterate_contracts_without_specified_block(self):
    test_contracts = [{
      "address": "0x1",
      "blockNumber": 1,
      "id": 1
    }, {
      "address": "0x2",
      "blockNumber": 1,
      "id": 2
    }]
    test_contract_blocks = [
      {"id": 1, "name": "tx_test_block", "value": 1},
    ]
    self.client.bulk_index(index=TEST_CONTRACT_BLOCK_INDEX, docs=test_contract_blocks)
    self.client.bulk_index(index=TEST_CONTRACTS_INDEX, docs=test_contracts)

    contracts_without_max_block = [
      c["_id"]
      for contracts in self.contracts_iterator._iterate_contracts(partial_query="WHERE address IS NOT NULL")
      for c in contracts
    ]
    contracts_with_zero_max_block = [
      c["_id"]
      for contracts in self.contracts_iterator._iterate_contracts(0, partial_query="WHERE address IS NOT NULL")
      for c in contracts
    ]

    self.assertCountEqual(contracts_without_max_block, ['1', '2'])
    self.assertCountEqual(contracts_with_zero_max_block, ['2'])

  def test_iterate_contracts_use_fields(self):
    test_fields = ["field1", "field2"]
    self.contracts_iterator.client.iterate = MagicMock()
    self.contracts_iterator._iterate_contracts(partial_query="WHERE address IS NOT NULL", fields=test_fields)
    self.contracts_iterator.client.iterate.assert_called_with(index=ANY, query=ANY, fields=test_fields)

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
    assert transactions_request == \
      "(to = '0x1' AND blockNumber > 10 AND blockNumber <= 40)" + \
      " OR (to = '0x2' AND blockNumber > 30 AND blockNumber <= 40)"

  def test_create_transactions_request_empty_block(self):
    test_max_block = 40
    test_contracts = [
      {"_source": {"address": "0x1"}},
    ]
    transactions_request = self.contracts_iterator._create_transactions_request(
      test_contracts,
      test_max_block
    )
    assert transactions_request == "(to = '0x1')"

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
    assert transactions_request == "(to in('0x1', '0x2') AND blockNumber > 10 AND blockNumber <= 40)"

  def test_iterate_transactions_by_query(self):
    self.contracts_iterator._create_transactions_request = MagicMock(return_value="id IS NOT NULL")
    documents = [{
      "id": 1,
      'to': "0x1",
      "from": "0x1",
    }, {
      "id": 2,
      'to': "0x1",
      "from": "0x2",
    }]
    self.client.bulk_index(index=TEST_TRANSACTIONS_INDEX, docs=documents)
    targets = [{"_source": {"address": "0x1"}}]
    transactions = self.contracts_iterator._iterate_transactions(targets, 0, "WHERE from = '0x1'")
    transactions = [t["_id"] for transactions_list in transactions for t in transactions_list]
    self.assertCountEqual(transactions, ['1'])

  def test_iterate_transactions_by_targets_select_unprocessed_transactions(self):
    test_contracts = [{
      'to': "0x1",
      "blockNumber": 1,
      "id": 1
    }, {
      'to': "0x1",
      "blockNumber": 2,
      "id": 2
    }, {
      'to': "0x1",
      "blockNumber": 3,
      "id": 3
    }]
    self.client.bulk_index(index=TEST_TRANSACTIONS_INDEX, docs=test_contracts)
    targets = [{"_source": {"address": "0x1", "tx_test_block": 1}}]
    test_query = "WHERE id IS NOT NULL"
    transactions = [c for c in self.contracts_iterator._iterate_transactions(targets, 2, test_query)]
    transactions = [t["_id"] for transactions_list in transactions for t in transactions_list]
    self.assertCountEqual(transactions, ['2'])

  def test_iterate_transactions_use_fields(self):
    test_fields = ["field1", "field2"]
    self.contracts_iterator._create_transactions_request = MagicMock()
    self.contracts_iterator.client.iterate = MagicMock()
    self.contracts_iterator._iterate_transactions([], 0, partial_query="WHERE to IS NOT NULL", fields=test_fields)
    self.contracts_iterator.client.iterate.assert_called_with(index=ANY, query=ANY, fields=test_fields)

  def test_save_max_block(self):
    test_max_block = 100
    contracts = [{'address': "0x{}".format(i), "id": i} for i in range(1, 4)]
    self.client.bulk_index(TEST_CONTRACTS_INDEX, contracts)
    self.contracts_iterator._save_max_block(["0x1", "0x3"], test_max_block)

    flags = self.client.search(TEST_CONTRACT_BLOCK_INDEX, fields=[], query="WHERE name = '{}' AND value = {}".format(
      "tx_test_block",
      test_max_block
    ))
    flags = [flag["_id"] for flag in flags]
    self.assertCountEqual(flags, ["0x1", "0x3"])

  def test_get_max_block(self):
    test_contracts = [{
      "number": 0,
      "id": 0
    }, {
      "number": 1,
      "id": 1
    }]
    self.client.bulk_index(index=TEST_BLOCKS_INDEX, docs=test_contracts)
    max_block = self.contracts_iterator._get_max_block()
    print(max_block)
    assert max_block == 1
    assert type(max_block) == int

  def test_get_max_block_by_a_query(self):
    block_flags = [{
      "id": 0,
      "name": "trace",
      "value": True
    }]
    self.client.bulk_index(index=TEST_BLOCK_FLAGS_INDEX, docs=block_flags)
    max_block = self.contracts_iterator._get_max_block({"trace": 1})
    assert max_block == 0

  def test_get_max_block_in_empty_index(self):
    max_block = self.contracts_iterator._get_max_block({}, 1)
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

TEST_BLOCKS_INDEX = "test_ethereum_blocks"
TEST_BLOCK_FLAGS_INDEX = "test_ethereum_block_flags"
TEST_CONTRACTS_INDEX = "test_ethereum_contract"
TEST_TRANSACTIONS_INDEX = "test_ethereum_transaction"
TEST_CONTRACT_BLOCK_INDEX = "test_ethereum_contract_flags"
config.INDICES.update({
  "block": TEST_BLOCKS_INDEX
})