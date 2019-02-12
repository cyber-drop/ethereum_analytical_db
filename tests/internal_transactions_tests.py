import unittest
from tests.test_utils import mockify, TestClickhouse, parity
from operations.internal_transactions import *
from operations.internal_transactions import \
  _get_parity_url_by_block, \
  _get_traces_sync, \
  _make_trace_requests, \
  _merge_block, \
  _make_transactions_requests, \
  _send_jsonrpc_request
from operations import internal_transactions
import json
import httpretty
from unittest.mock import MagicMock, patch, call, Mock, ANY
from clients.custom_clickhouse import CustomClickhouse
from operations.indices import ClickhouseIndices
import os
from pprint import pprint

class InternalTransactionsTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestClickhouse()
    self.indices = {
      "block": TEST_BLOCKS_INDEX,
      "transaction": TEST_TRANSACTIONS_INDEX,
      "internal_transaction": TEST_INTERNAL_TRANSACTIONS_INDEX,
      "miner_transaction": TEST_MINER_TRANSACTIONS_INDEX,
      "block_flag": TEST_BLOCKS_TRACES_EXTRACTED_INDEX
    }
    self.client.prepare_indices(self.indices)
    self.parity_hosts = [(None, None, "http://localhost:8545")]
    self.internal_transactions = ClickhouseInternalTransactions(self.indices, parity_hosts=self.parity_hosts)

  def test_split_on_chunks(self):
    """
    Test splitting on chunks
    """
    test_list = []
    test_chunks_number = 10
    test_chunks = []
    split_mock = MagicMock(return_value=test_chunks)
    with patch('utils.split_on_chunks', split_mock):
      chunks = self.internal_transactions._split_on_chunks(test_list, test_chunks_number)
      split_mock.assert_called_with(test_list, test_chunks_number)
      assert chunks == test_chunks

  def test_get_parity_url_by_block(self):
    """
    Test getting url for block by specified config
    """
    parity_hosts = [
      (0, 100, "url1"),
      (100, 200, "url2")
    ]
    assert _get_parity_url_by_block(parity_hosts, 1) == "url1"
    assert _get_parity_url_by_block(parity_hosts, 101) == "url2"
    assert _get_parity_url_by_block(parity_hosts, 1000) == None

  def test_get_default_parity_url(self):
    """
    Test getting url for block inside of specified opened ranges
    """
    parity_hosts = [
      (10, 100, "url1"),
      (None, 10, "url2"),
      (100, None, "url3")
    ]
    assert _get_parity_url_by_block(parity_hosts, 9) == "url2"
    assert _get_parity_url_by_block(parity_hosts, 10000) == "url3"

  def _make_requests(self, method, check):
    """
    Test making trace requests for each block
    """
    parity_hosts = [
      (TEST_BLOCK_NUMBER, TEST_BLOCK_NUMBER + 3, "http://localhost:8545"),
      (TEST_BLOCK_NUMBER + 3, None, "http://localhost:8546")
    ]
    requests = method(parity_hosts, [TEST_BLOCK_NUMBER + i for i in range(10)])
    requests_to_node = requests["http://localhost:8546"]
    for i, request in enumerate(requests_to_node):
      check(self, i, request)

  def test_make_transactions_requests(self):
    def check(self, index, request):
      assert request["jsonrpc"] == "2.0"
      assert request["id"] == "transactions_{}".format(TEST_BLOCK_NUMBER + index + 3)
      assert request["method"] == "eth_getBlockByNumber"
      self.assertSequenceEqual(request["params"], [hex(TEST_BLOCK_NUMBER + index + 3), True])

    self._make_requests(_make_transactions_requests, check)

  def test_make_trace_requests(self):
    def check(self, index, request):
      assert request["jsonrpc"] == "2.0"
      assert request["id"] == "trace_{}".format(TEST_BLOCK_NUMBER + index + 3)
      assert request["method"] == "trace_block"
      self.assertSequenceEqual(request["params"], [hex(TEST_BLOCK_NUMBER + index + 3)])

    self._make_requests(_make_trace_requests, check)

  def test_merge_block(self):
    test_transactions = [
      {"hash": "0x1", "blockHash": "0x1", "test": True},
      {"hash": "0x2", "blockHash": "0x1", "test_not_listed": False},
      {"hash": None, "blockHash": "0x1"},
      {"hash": "0x1", "blockHash": "0x2", "test_2": True},
    ]
    test_internal_transactions = [
      {"transactionHash": "0x1", "test": False, "blockHash": "0x1"},
      {"transactionHash": "0x2", "blockHash": "0x1"},
      {"transactionHash": "0x1", "internal_test": True, "blockHash": "0x1"},
      {"transactionHash": None, "blockHash": "0x1"},
      {"transactionHash": "0x1", "test_2": False, "blockHash": "0x2"},
    ]
    merged_block = _merge_block(test_internal_transactions, test_transactions, ["test", "test_2"])
    self.assertSequenceEqual(merged_block, [
      {"transactionHash": "0x1", "test": True, "blockHash": "0x1"},
      {"transactionHash": "0x2", "blockHash": "0x1"},
      {"transactionHash": "0x1", "internal_test": True, "blockHash": "0x1"},
      {"transactionHash": None, "blockHash": "0x1"},
      {"transactionHash": "0x1", "test_2": True, "blockHash": "0x2"},
    ])

  @httpretty.activate
  def test_send_jsonrpc_request(self):
    test_request = [
      {"id": "1", "params": "some"},
      {"id": "2", "params": "other"},
      {"id": "3", "params": "error"}
    ]
    test_response = ["result_1", "result_2", "result_3"]
    test_url = "http://localhost:8545/"
    httpretty.register_uri(
      httpretty.POST,
      test_url,
      body=json.dumps([
        {"id": "2", "result": {"test": ["result_2", "result_3"]}},
        {"id": "1", "result": {"test": ["result_1"]}},
        {"id": "3", "error": True}
      ])
    )
    response = _send_jsonrpc_request(
      test_url,
      test_request,
      lambda x: x.get("result", {"test": []}).get("test", [])
    )
    self.assertCountEqual(response, test_response)

  def test_get_traces_sync(self):
    test_parity_hosts = "hosts"
    test_blocks = "blocks"
    test_urls = ["url1", "url2"]
    test_trace_requests = {
      test_urls[0]: "trace1",
      test_urls[1]: "trace2"
    }
    test_transactions_requests = {
      test_urls[1]: "transactions2",
      test_urls[0]: "transactions1"
    }
    test_trace_response = {
      "result": ["trace"]
    }
    test_transactions_response = {
      "result": {
        "transactions": ["transactions"]
      }
    }

    make_trace_requests_mock = MagicMock(return_value=test_trace_requests)
    make_transactions_requests_mock = MagicMock(return_value=test_transactions_requests)
    return_response_mock = MagicMock(side_effect=[test_trace_response, test_transactions_response] * 2)
    send_jsonrpc_request_mock = MagicMock(side_effect=lambda x, y, getter: getter(return_response_mock()))
    merge_block_mock = MagicMock(side_effect=[
      ["merge1"],
      ["merge2"]
    ])

    process = Mock(
      trace_request=make_trace_requests_mock,
      transaction_request=make_transactions_requests_mock,
      send=send_jsonrpc_request_mock,
      merge=merge_block_mock
    )

    with patch("operations.internal_transactions._make_trace_requests", make_trace_requests_mock), \
         patch("operations.internal_transactions._make_transactions_requests", make_transactions_requests_mock), \
         patch("operations.internal_transactions._send_jsonrpc_request", send_jsonrpc_request_mock), \
         patch("operations.internal_transactions._merge_block", merge_block_mock):
      result = _get_traces_sync(test_parity_hosts, test_blocks)

      calls = [
        call.trace_request(test_parity_hosts, test_blocks),
        call.transaction_request(test_parity_hosts, test_blocks)
      ]
      for url, trace_request in test_trace_requests.items():
        transaction_request = test_transactions_requests[url]
        calls += [call.send(url, trace_request, ANY), call.send(url, transaction_request, ANY)]
        calls += [call.merge(["trace"], ["transactions"], ["gasUsed", "gasPrice"])]
      process.assert_has_calls(calls)
      self.assertSequenceEqual(result, ["merge1", "merge2"])

  def test_get_traces(self):
    """
    Test parallel process of getting traces
    """
    test_hosts = []
    test_traces = ["trace" + str(i + 1) for i in range(100)]
    test_blocks = [str(i + 1) for i in range(100)]
    test_chunks = [[str(j*10 + i + 1) for i in range(10)] for j in range(10)]
    test_chunks_with_parameters = [(test_hosts, chunk) for chunk in test_chunks]
    test_map_result = [
      ["trace" + str(j*10 + i + 1) for i in range(10)]
      for j in range(10)
    ]
    self.internal_transactions.parity_hosts = test_hosts
    self.internal_transactions._split_on_chunks = MagicMock(return_value=test_chunks)
    self.internal_transactions.pool.starmap = MagicMock(return_value=test_map_result)
    process = Mock(
      split=self.internal_transactions._split_on_chunks,
      map=self.internal_transactions.pool.starmap
    )

    traces = self.internal_transactions._get_traces(test_blocks)

    process.assert_has_calls([
      call.split(test_blocks, 10),
      call.map(_get_traces_sync, test_chunks_with_parameters)
    ])
    self.assertCountEqual(test_traces, traces)

  def test_set_trace_hashes(self):
    """
    Test setting trace hashes for each transaction with ethereum transaction hash
    """
    transactions = [{
      "transactionHash": "0x1"
    }, {
      "transactionHash": "0x1"
    }, {
      "transactionHash": "0x2"
    }, {
      "transactionHash": "0x1"
    }]
    self.internal_transactions._set_trace_hashes(transactions)
    assert transactions[0]["hash"] == "0x1.0"
    assert transactions[1]["hash"] == "0x1.1"
    assert transactions[2]["hash"] == "0x2.0"
    assert transactions[3]["hash"] == "0x1.2"

  def test_set_trace_hashes_for_reward(self):
    """
    Test setting trace hashes for each mining transaction
    """
    transactions = [{
      "transactionHash": None,
      "blockHash": "0x1"
    }, {
      "transactionHash": None,
      "blockHash": "0x1"
    }]
    self.internal_transactions._set_trace_hashes(transactions)
    assert transactions[0]["hash"] == "0x1.0"
    assert transactions[1]["hash"] == "0x1.1"

  def test_set_parent_error_root_node(self):
    """
    Test set parent_error field for each transaction in trace if root is corrupted
    """
    trace = [{
      "transactionHash": "0x1",
      "error": "Out of gas",
      "traceAddress": []
    }, {
      "transactionHash": "0x1",
      "traceAddress": [1]
    }, {
      "transactionHash": "0x2",
      "traceAddress": [1]
    }]
    self.internal_transactions._set_parent_errors(trace)
    assert trace[1]["parent_error"]
    assert "parent_error" not in trace[2].keys()

  def test_set_parent_error_leaf(self):
    """
    Test skip all transactions in trace if error is in leaf
    """
    trace = [{
      "transactionHash": "0x1",
      "traceAddress": []
    }, {
      "transactionHash": "0x1",
      "error": "Out of gas",
      "traceAddress": [1]
    }]
    self.internal_transactions._set_parent_errors(trace)
    assert "parent_error" not in trace[0].keys()

  def test_set_parent_error_internal_node(self):
    """
    Test set parent_error field for each transaction in branch if root of branch is corrupted
    """
    trace = [{
      "transactionHash": "0x1",
      "traceAddress": []
    }, {
      "transactionHash": "0x1",
      "error": "Out of gas",
      "traceAddress": [1]
    }, {
      "transactionHash": "0x1",
      "traceAddress": [1, 2]
    }, {
      "transactionHash": "0x1",
      "traceAddress": [2]
    }]
    self.internal_transactions._set_parent_errors(trace)
    assert "parent_error" in trace[2].keys()
    assert "parent_error" not in trace[3].keys()
    assert "parent_error" not in trace[0].keys()
    assert "parent_error" not in trace[1].keys()

  def test_set_parent_error_multiple_internal_nodes(self):
    """
    Test set parent errors in more than one internal nodes
    """
    trace = [{
      "transactionHash": "0x1",
      "error": "Out of gas",
      "traceAddress": [1, 3]
    }, {
      "transactionHash": "0x1",
      "error": "Out of gas",
      "traceAddress": [1, 2]
    }, {
      "transactionHash": "0x1",
      "traceAddress": [1, 2, 3]
    }]
    self.internal_transactions._set_parent_errors(trace)
    assert "parent_error" in trace[-1].keys()

  def test_preprocess_internal_transaction_with_empty_field(self):
    self.internal_transactions._preprocess_internal_transaction({"action": None})
    assert True

  def test_preprocess_internal_transaction_value(self):
    transaction = self.internal_transactions._preprocess_internal_transaction({"value": hex(int(50.001851 * 1e18))})
    assert transaction["value"] == 50.001851

  def test_preprocess_internal_transaction_gas_used(self):
    transaction = self.internal_transactions._preprocess_internal_transaction({"gasUsed": hex(10000)})
    print(transaction["gasUsed"])
    assert transaction["gasUsed"] == 10000

  def test_preprocess_internal_transaction_gas_price(self):
    transaction = self.internal_transactions._preprocess_internal_transaction({"gasPrice": hex(int(10.1 * 10**18))})
    assert transaction["gasPrice"] == 10.1

  def test_preprocess_internal_transaction_empty_value(self):
    transaction = self.internal_transactions._preprocess_internal_transaction({"value": "0x"})
    assert transaction["value"] == 0

  def test_save_internal_transactions(self):
    """
    Test saving given transactions from trace
    """
    test_trace = [{"transactionHash": "0x0", "hash": "0x0.{}".format(i)} for i in range(10)]
    test_preprocessed_trace = [{
      "hash": "0x{}".format(i),
      "transactionHash": '0x0'
    } for i in range(10)]
    test_transactions_ids = [transaction["hash"] for transaction in test_preprocessed_trace]
    test_transactions_bodies = [{key: value for key, value in transaction.items() if key is not "hash"} for transaction in test_preprocessed_trace]
    self.internal_transactions._preprocess_internal_transaction = MagicMock(side_effect=test_preprocessed_trace)

    self.internal_transactions._save_internal_transactions(test_trace)

    for transaction in test_trace:
      self.internal_transactions._preprocess_internal_transaction.assert_any_call(transaction)

    internal_transactions = self.client.search(index=TEST_INTERNAL_TRANSACTIONS_INDEX, fields=["transactionHash"])
    internal_transactions_bodies = [transaction["_source"] for transaction in internal_transactions]
    internal_transactions_ids = [transaction["_id"] for transaction in internal_transactions]
    self.assertCountEqual(internal_transactions_ids, test_transactions_ids)
    self.assertCountEqual(internal_transactions_bodies, test_transactions_bodies)

  def test_save_internal_transactions_ignore_rewards(self):
    """
    Test ignoring transactions from trace which are not attached to any ethereum transaction
    """
    trace = [{"transactionHash": None, "hash": "0x1"}]
    self.internal_transactions._save_internal_transactions(trace)
    internal_transactions = self.client.search(index=TEST_INTERNAL_TRANSACTIONS_INDEX, fields=[])
    assert not len(internal_transactions)

  def test_save_miner_transaction(self):
    """
    Test saving transactions which are not attached to any ethereum transaction
    """
    trace = [{"transactionHash": None, "hash": "0x1"}, {"transactionHash": "0x1"}]
    self.internal_transactions._save_miner_transactions(trace)
    miner_transactions = self.client.search(index=TEST_INTERNAL_TRANSACTIONS_INDEX, fields=["transactionHash"])
    assert len(miner_transactions) != 0
    assert miner_transactions[0]["_id"] == "0x1"
    self.assertCountEqual(miner_transactions[0]["_source"], {"transactionHash": None})

  def test_save_genesis(self):
    test_genesis = [{"hash": "1", "to": "0x"}]
    with open('test_genesis.json', "w") as file:
      file.write(json.dumps(test_genesis))

    self.internal_transactions._save_genesis_block('test_genesis.json')
    genesis = self.client.search(index=TEST_INTERNAL_TRANSACTIONS_INDEX, fields=["to"])

    os.remove("test_genesis.json")

    assert genesis[0]["_source"]["to"] == "0x"
    assert genesis[0]["_id"] == "1"

  def test_extract_traces_chunk(self):
    """
    Test process of extraction internal transactions by a given blocks chunk
    """
    test_blocks = ["0x{}".format(i) for i in range(10)]
    test_traces = [{"transactionHash": "0x{}".format(i % 3)} for i in range(10)]
    mockify(self.internal_transactions, {
      "_get_traces": MagicMock(return_value=test_traces)
    }, ["_extract_traces_chunk"])
    process = Mock(
      get_traces=self.internal_transactions._get_traces,
      set_hashes=self.internal_transactions._set_trace_hashes,
      save_traces=self.internal_transactions._save_traces,
      save_transactions=self.internal_transactions._save_internal_transactions,
      save_rewards=self.internal_transactions._save_miner_transactions
    )

    self.internal_transactions._extract_traces_chunk(test_blocks)

    calls = [
      call.get_traces(test_blocks),
      call.set_hashes(test_traces),
      call.save_transactions(test_traces),
      call.save_rewards(test_traces),
      call.save_traces(test_blocks)
    ]
    process.assert_has_calls(calls)

  def test_extract_traces_chunk_set_parent_error(self):
    """
    Test setting parent error in a process of extraction by a given block chunk
    """
    test_blocks = ["0x1"]
    test_traces = []
    mockify(self.internal_transactions, {
      "_get_traces": MagicMock(return_value=test_traces)
    }, ["_extract_traces_chunk"])
    process = Mock(
      save_errors=self.internal_transactions._set_parent_errors,
      save_traces=self.internal_transactions._save_internal_transactions
    )

    self.internal_transactions._extract_traces_chunk(test_blocks)

    calls = [
      call.save_errors(test_traces),
      call.save_traces(test_traces)
    ]
    process.assert_has_calls(calls)

  def test_extract_traces_chunk_extract_genesis(self):
    test_blocks = [0]
    test_traces = []
    test_blocks_no_genesis = [1]
    mockify(self.internal_transactions, {
      "_get_traces": MagicMock(return_value=test_traces)
    }, ["_extract_traces_chunk"])

    self.internal_transactions._extract_traces_chunk(test_blocks_no_genesis)
    self.internal_transactions._save_genesis_block.assert_not_called()

    self.internal_transactions._extract_traces_chunk(test_blocks)
    self.internal_transactions._save_genesis_block.assert_called_with()

  def test_extract_traces(self):
    """
    Test overall extraction process
    """
    test_chunks = [list(range(5)), list(range(5, 10))]
    test_chunks_from_elasticsearch = [[{"_source": {"number": block}} for block in chunk] for chunk in test_chunks]
    self.internal_transactions._iterate_blocks = MagicMock(return_value=test_chunks_from_elasticsearch)
    self.internal_transactions._extract_traces_chunk = MagicMock()
    process = Mock(
      iterate=self.internal_transactions._iterate_blocks,
      extract=self.internal_transactions._extract_traces_chunk
    )

    self.internal_transactions.extract_traces()

    calls = [call.iterate()]
    for chunk in test_chunks:
      calls.append(call.extract(chunk))
    process.assert_has_calls(calls)

  def test_iterate_blocks(self):
    self.internal_transactions.parity_hosts = [(0, 4, "http://localhost:8545"), (5, None, "http://localhost:8545")]
    blocks = [{'number': i, 'id': i} for i in range(1, 6)]
    flags = [
      {'name': 'traces_extracted', 'value': True, "id": 3},
      {'name': 'traces_extracted', 'value': True, "id": 2},
      {'name': 'traces_extracted', 'value': None, 'id': 2},
      {"name": "other_flag", "value": True, "id": 1}
    ]
    self.client.bulk_index(index=TEST_BLOCKS_INDEX, docs=blocks)
    self.client.bulk_index(index=TEST_BLOCKS_TRACES_EXTRACTED_INDEX, docs=flags)
    iterator = self.internal_transactions._iterate_blocks()
    blocks = next(iterator)
    blocks = [block["_source"]["number"] for block in blocks]
    self.assertCountEqual(blocks, [1, 2, 5])

  def test_save_traces(self):
    self.internal_transactions._save_traces([123])
    block = \
    self.client.search(index=TEST_BLOCKS_TRACES_EXTRACTED_INDEX, query="WHERE id = '123'", fields=["name", "value"])[0][
      '_source']
    assert block['value']
    assert block['name'] == 'traces_extracted'

  @parity
  def test_process(self):
    """
    Test extraction on a real data
    """
    test_start_block = 7000000
    test_end_block = test_start_block + 3
    test_blocks = [{"id": block, "number": block} for block in range(test_start_block, test_end_block)]
    self.client.bulk_index(
      docs=test_blocks,
      index=TEST_BLOCKS_INDEX
    )

    self.internal_transactions.extract_traces()
    transactions = self.client.search(index=TEST_INTERNAL_TRANSACTIONS_INDEX, fields=["from", "to", "author", "value"])
    pprint(transactions)
    assert len(transactions) == 873

TEST_TRANSACTIONS_NUMBER = 10
TEST_BLOCK_NUMBER = 3068185
TEST_BIG_TRANSACTIONS_NUMBER = TEST_TRANSACTIONS_NUMBER * 10
TEST_TRANSACTIONS_INDEX = 'test_ethereum_transactions'
TEST_INTERNAL_TRANSACTIONS_INDEX = 'test_ethereum_internal_transactions'
TEST_BLOCKS_INDEX = "test_ethereum_blocks"
TEST_MINER_TRANSACTIONS_INDEX = 'test_ethereum_miner_transactions'
TEST_TRANSACTION_HASH = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_INPUT = '0xb1631db29e09ec5581a0ec398f1229abaf105d3524c49727621841af947bdc44'
TEST_INCORRECT_TRANSACTION_HASH = "0x"
TEST_BLOCKS_TRACES_EXTRACTED_INDEX = "test_ethereum_block_traces_extracted"