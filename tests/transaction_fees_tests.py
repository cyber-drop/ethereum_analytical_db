from tests.test_utils import TestClickhouse, mockify
import unittest
from operations.transaction_fees import ClickhouseTransactionFees, _extract_gas_used_sync, _extract_transactions_for_blocks_sync
import httpretty
from unittest.mock import MagicMock, call, Mock, patch
import json
from web3 import Web3
from operations import transaction_fees

TEST_BLOCK_INDEX = 'test_block'
TEST_TRANSACTION_INDEX = 'test_transaction'
TEST_TRANSACTION_FEE_INDEX = 'test_transaction_fee'
TEST_CONTRACT_INDEX = 'test_contract'
TEST_BLOCK_FLAG_INDEX = 'test_block_flag'

class TransactionFeesTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestClickhouse()
    self.indices = {
      "block": TEST_BLOCK_INDEX,
      "internal_transaction": TEST_TRANSACTION_INDEX,
      "transaction_fee": TEST_TRANSACTION_FEE_INDEX,
      "block_flag": TEST_BLOCK_FLAG_INDEX
    }
    self.client.prepare_indices(self.indices)
    self.transaction_fees = ClickhouseTransactionFees(self.indices, parity_host="http://localhost:8545")

  def test_iterate_blocks(self):
    blocks = [
      {"id": 1, "number": 1},
      {"id": 2, "number": 2},
      {"id": 3, "number": 3}
    ]
    block_flags = [
      {"id": 1, "name": "other_flag", "value": True},
      {"id": 2, "name": "fees_extracted", "value": True},
      {"id": 3, "name": "fees_extracted", "value": True},
      {"id": 3, "name": "fees_extracted", "value": None},
    ]
    self.client.bulk_index(self.indices["block"], blocks)
    self.client.bulk_index(self.indices["block_flag"], block_flags)
    blocks = next(self.transaction_fees._iterate_blocks())
    blocks = [block["_source"]["number"] for block in blocks]
    self.assertCountEqual(blocks, [1, 3])

  @httpretty.activate
  def test_extract_transactions_for_blocks_sync(self):
    test_blocks = [1, 2]
    test_transactions = [{
      "hash": "0x01",
      "gasPrice": 100,
      "blockNumber": 123
    }]
    test_response = [{
      "hash": t["hash"],
      "gasPrice": hex(Web3.toWei(t["gasPrice"], "ether")),
      "blockNumber": hex(t["blockNumber"])
    } for t in test_transactions]
    httpretty.register_uri(
      httpretty.POST,
      "http://localhost:8545/",
      body=json.dumps({
        "id": 1,
        "jsonrpc": "2.0",
        "result": {
          "transactions": test_response
        }
      })
    )
    result = _extract_transactions_for_blocks_sync(test_blocks, "http://localhost:8545")
    self.assertCountEqual(result, test_transactions + test_transactions)
    assert json.loads(httpretty.last_request().body.decode("utf-8"))["params"][-1]

  def test_extract_transactions_for_blocks(self):
    blocks = ["block" + str(i) for i in range(100)]
    chunks = [["block1"], ["block2"]]
    transactions = [["transactions1"], ["transactions2"]]
    split_mock = MagicMock(return_value=chunks)
    self.transaction_fees.pool.map = MagicMock(return_value=transactions)
    with patch('utils.split_on_chunks', split_mock):
      response = self.transaction_fees._extract_transactions_for_blocks(blocks)
      split_mock.assert_called_with(blocks, 10)
      self.transaction_fees.pool.map.assert_called_with(transaction_fees._extract_transactions_for_blocks_sync, chunks)
      self.assertSequenceEqual(["transactions1", "transactions2"], response)

  @httpretty.activate
  def test_extract_gas_used_sync(self):
    test_gas_used = 21000
    test_transaction_hashes = ["0x01", "0x02"]
    httpretty.register_uri(
      httpretty.POST,
      "http://localhost:8545/",
      body=json.dumps({
        "id": 1,
        "jsonrpc": "2.0",
        "result": {
          "gasUsed": hex(test_gas_used)
        }
      })
    )
    gas_used = _extract_gas_used_sync(test_transaction_hashes, "http://localhost:8545")
    self.assertSequenceEqual(gas_used, {"0x01": test_gas_used, "0x02": test_gas_used})

  def test_extract_gas_used(self):
    hashes = ["hash" + str(i) for i in range(100)]
    chunks = [["hash1"], ["hash2"]]
    gas_used = [{"hash1": "gas1"}, {"hash2": "gas2"}]
    split_mock = MagicMock(return_value=chunks)
    self.transaction_fees.pool.map = MagicMock(return_value=gas_used)
    with patch('utils.split_on_chunks', split_mock):
      response = self.transaction_fees._extract_gas_used(hashes)
      split_mock.assert_called_with(hashes, 10)
      self.transaction_fees.pool.map.assert_called_with(transaction_fees._extract_gas_used_sync, chunks)
      self.assertSequenceEqual({"hash1": "gas1", "hash2": "gas2"}, response)

  def test_update_transactions(self):
    test_transaction_fees = [{"hash": "0x01", "gasUsed": 100, "gasPrice": 2.1, "test": True}]
    test_database_transactions = [{"id": t["hash"] + ".0", "gasUsed": t["gasUsed"], "gasPrice": t["gasPrice"]} for t in test_transaction_fees]
    self.client.bulk_index(docs=test_database_transactions, index=TEST_TRANSACTION_FEE_INDEX)
    self.transaction_fees._update_transactions(test_transaction_fees)
    result = self.client.search(index=TEST_TRANSACTION_FEE_INDEX, fields=["gasPrice", "gasUsed"])
    assert len(result) == 1
    assert result[0]["_id"] == "0x01.0"
    assert result[0]["_source"]["gasPrice"] == 2.1
    assert result[0]["_source"]["gasUsed"] == 100
    assert "test" not in result[0]["_source"]

  def test_update_empty_transactions(self):
    self.transaction_fees._update_transactions([])

  def test_count_transaction_fees(self):
    test_blocks = [1234, 1235, 1236]
    test_transactions = [{
      "blockNumber": 1234,
      "gasPrice": 1,
      "gasUsed": 2
    }, {
      "blockNumber": 1234,
      "gasPrice": 2,
      "gasUsed": 3
    }, {
      "blockNumber": 1235,
      "gasPrice": 3,
      "gasUsed": 4
    }]
    result = self.transaction_fees._count_transaction_fees(test_transactions, test_blocks)
    self.assertSequenceEqual(result, {
      1234: 1*2 + 2*3,
      1235: 3*4,
      1236: 0
    })

  def test_update_blocks(self):
    test_block_numbers = [1234]
    test_blocks = [{"number": b, "id": b} for b in test_block_numbers]
    self.client.bulk_index(docs=test_blocks, index=TEST_BLOCK_INDEX)
    self.transaction_fees._update_blocks(test_block_numbers)
    result = self.client.search(index=TEST_BLOCK_FLAG_INDEX, query="WHERE id = '1234' AND name = 'fees_extracted' AND value IS NOT NULL", fields=[])
    assert len(result) == 1

  def test_extract_transaction_fees(self):
    test_blocks = [[{"_source": {"number": 1}}, {"_source": {"number": 2}}]]
    test_all_blocks = [b for l in test_blocks for b in l]
    test_all_transactions = [
      {"hash": "0x01"},
      {"hash": "0x02"}
    ]
    test_gas_used = {"0x01": 100, "0x02": 200}
    mockify(self.transaction_fees, {
      "_iterate_blocks": MagicMock(return_value=test_blocks),
      "_extract_transactions_for_blocks": MagicMock(return_value=test_all_transactions),
      "_extract_gas_used": MagicMock(return_value=test_gas_used)
    }, "extract_transaction_fees")

    process = Mock(
      iterate=self.transaction_fees._iterate_blocks,
      extract=self.transaction_fees._extract_transactions_for_blocks,
      extract_gas=self.transaction_fees._extract_gas_used,
      update_transactions=self.transaction_fees._update_transactions,
      update_blocks=self.transaction_fees._update_blocks
    )

    self.transaction_fees.extract_transaction_fees()

    calls = [call.iterate()]
    calls += [call.extract([block["_source"]["number"] for block in test_all_blocks])]
    calls += [call.extract_gas([transaction["hash"] for transaction in test_all_transactions])]
    calls += [call.update_transactions(test_all_transactions)]
    calls += [call.update_blocks([block["_source"]["number"] for block in test_all_blocks])]

    for transaction in test_all_transactions:
      assert transaction["gasUsed"] == test_gas_used[transaction["hash"]]

    process.assert_has_calls(calls)