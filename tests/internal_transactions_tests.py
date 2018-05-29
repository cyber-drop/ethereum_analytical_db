import unittest
from test_utils import TestElasticSearch
from internal_transactions import *
from internal_transactions import _get_parity_url_by_block, _get_traces_sync, _make_trace_requests
import internal_transactions
import random
import requests
import json
from multiprocessing import Pool
from tqdm import *
import httpretty
from test_constants import TEST_BLOCK_TRACES
from unittest.mock import MagicMock, patch, call, Mock

class InternalTransactionsTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_fast_index(TEST_TRANSACTIONS_INDEX)
    self.client.recreate_fast_index(TEST_INTERNAL_TRANSACTIONS_INDEX, doc_type='itx')
    self.client.recreate_index(TEST_MINER_TRANSACTIONS_INDEX)
    self.parity_hosts = [(None, None, "http://localhost:8545")]
    self.internal_transactions = InternalTransactions({
      "transaction": TEST_TRANSACTIONS_INDEX,
      "internal_transaction": TEST_INTERNAL_TRANSACTIONS_INDEX,
      "miner_transaction": TEST_MINER_TRANSACTIONS_INDEX
    }, parity_hosts=self.parity_hosts)

  def test_split_on_chunks(self):
    test_list = list(range(10))
    test_chunks = list(self.internal_transactions._split_on_chunks(test_list, 3))
    self.assertSequenceEqual(test_chunks, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])

  def test_iterate_blocks(self):
    self.internal_transactions.parity_hosts = [(0, 4, "http://localhost:8545"), (5, None, "http://localhost:8545")]
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'blockNumber': 1}, id=1, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'blockNumber': 2}, id=2, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'blockNumber': 3, 'trace': True}, id=3, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'blockNumber': 4}, id=4, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'blockNumber': 5}, id=5, refresh=True)
    blocks = self.internal_transactions._iterate_blocks()
    self.assertCountEqual(blocks, [1, 2, 5])

  def test_iterate_transactions(self):
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to_contract': False, 'blockNumber': 1}, id=1, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to_contract': True, 'trace': {'test': 1}, 'blockNumber': 2}, id=2, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to_contract': True, 'blockNumber': 1}, id=3, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to_contract': True, 'blockNumber': 3}, id=4, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'nottx', {'to_contract': True, 'blockNumber': 1}, id=5, refresh=True)
    iterator = self.internal_transactions._iterate_transactions(1)
    transactions = next(iterator)
    transactions = [transaction["_id"] for transaction in transactions]
    self.assertCountEqual(transactions, ['3'])

  def test_iterate_transactions_with_big_number_of_blocks(self):
    iterator = self.internal_transactions._iterate_transactions(list(range(1000)))
    [transactions for transactions in iterator]
    assert True

  def test_get_parity_url_by_block(self):
    parity_hosts = [
      (0, 100, "url1"),
      (100, 200, "url2")
    ]
    assert _get_parity_url_by_block(parity_hosts, 1) == "url1"
    assert _get_parity_url_by_block(parity_hosts, 101) == "url2"
    assert _get_parity_url_by_block(parity_hosts, 1000) == None

  def test_get_default_parity_url(self):
    parity_hosts = [
      (10, 100, "url1"),
      (None, 10, "url2"),
      (100, None, "url3")
    ]
    assert _get_parity_url_by_block(parity_hosts, 9) == "url2"
    assert _get_parity_url_by_block(parity_hosts, 10000) == "url3"

  def test_make_trace_requests(self):
    parity_hosts = [
      (TEST_BLOCK_NUMBER, TEST_BLOCK_NUMBER + 3, "http://localhost:8545"),
      (TEST_BLOCK_NUMBER + 3, None, "http://localhost:8546")
    ]
    requests = _make_trace_requests(parity_hosts, [TEST_BLOCK_NUMBER + i for i in range(10)])
    requests_to_node = requests["http://localhost:8546"]
    for i, request in enumerate(requests_to_node):
      assert request["jsonrpc"] == "2.0"
      assert request["id"] == TEST_BLOCK_NUMBER + i + 3
      assert request["method"] == "trace_block"
      self.assertSequenceEqual(request["params"], [hex(TEST_BLOCK_NUMBER + i + 3)])

  @httpretty.activate
  def test_get_traces_sync(self):
    test_parity_hosts = [
      (1, 2, "http://localhost:8545/"),
      (2, 3, "http://localhost:8546/"),
      (3, 4, "http://localhost:8547/")
    ]
    test_blocks = [1, 2, 3]
    test_requests = {
      "http://localhost:8545/": [{
        "id": "1",
        "params": "block_1"
      }],
      "http://localhost:8546/": [{
        "id": "2",
        "params": "block_2"
      }],
      "http://localhost:8547/": [{
        "id": "3",
        "params": "block_3"
      }]
    }
    test_responses = {
      "http://localhost:8545/": MagicMock(return_value=(200, {}, '[{"id": "1", "result": "transactions_1"}]')),
      "http://localhost:8546/": MagicMock(return_value=(200, {}, '[{"id": "2", "result": "transactions_2"}]')),
      "http://localhost:8547/": MagicMock(return_value=(200, {}, '[{"id": "3", "result": "transactions_3"}]'))
    }
    test_blocks_response = {
      "1": "transactions_1",
      "2": "transactions_2",
      "3":  "transactions_3"
    }

    make_trace_requests_mock = MagicMock(return_value=test_requests)
    for url, response in test_responses.items():
      httpretty.register_uri(
        httpretty.POST,
        url,
        body=response,
        content_type='application/json'
      )
    with patch('internal_transactions._make_trace_requests', make_trace_requests_mock):
      internal_transactions_response = internal_transactions._get_traces_sync(test_parity_hosts, test_blocks)

      internal_transactions._make_trace_requests.assert_called_with(test_parity_hosts, test_blocks)
      for url, request in test_requests.items():
        received_request = json.loads(test_responses[url].call_args[0][0].body.decode("utf-8"))
        self.assertSequenceEqual(received_request, request)
      self.assertSequenceEqual(internal_transactions_response, test_blocks_response)

  def test_get_traces(self):
    test_hosts = []
    test_traces = {str(i + 1): "trace" + str(i + 1) for i in range(100)}
    test_blocks = list(test_traces.keys())
    test_chunks = [[str(j*10 + i + 1) for i in range(10)] for j in range(10)]
    test_chunks_with_parameters = [(test_hosts, chunk) for chunk in test_chunks]
    test_map_result = [
      {str(j*10 + i + 1): "trace" + str(j*10 + i + 1) for i in range(10)}
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
    self.assertSequenceEqual(test_traces, traces)

  def test_set_trace_hashes(self):
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
    transactions = [{
      "transactionHash": None,
      "blockHash": "0x1"
    }]
    self.internal_transactions._set_trace_hashes(transactions)
    assert transactions[0]["hash"] == "0x1"

  def test_classify_trace(self):
    trace = [{
      "transactionHash": "0x0",
      "action": {
        "from": "0x0",
        "to": "0x1"
      }
    }, {
      "transactionHash": "0x0",
      "action": {
        "from": "0x1",
        "to": "0x0"
      }
    }, {
      "transactionHash": "0x0",
      "action": {
        "from": "0x0",
        "to": "0x3"
      }
    }, {
      "transactionHash": "0x0",
      "action": {
        "from": "0x0",
        "to": "0x0"
      }
    }, {
      "transactionHash": "0x0",
      "action": {
        "from": "0x0"
      }
    }, {
      "transactionHash": "0x1",
      "action": {
        "from": "0x0",
        "to": "0x1"
      }
    }]
    transactions = [{
      "_id": "0x0",
      "_source": {
        "from": "0x0",
        "to": "0x1"
      }
    },
    {
      "_id": "0x1",
      "_source": {
        "from": "0x1",
        "to": "0x2"
      }
    }]
    self.internal_transactions._classify_trace(transactions, trace)
    assert trace[0]["class"] == INPUT_TRANSACTION
    assert trace[1]["class"] == INTERNAL_TRANSACTION
    assert trace[2]["class"] == OUTPUT_TRANSACTION
    assert trace[3]["class"] == OTHER_TRANSACTION
    assert trace[4]["class"] == OTHER_TRANSACTION
    assert trace[5]["class"] == OTHER_TRANSACTION

  def test_classify_reward(self):
    trace = [{
      "transactionHash": None
    }, {
      "transactionHash": "0x1",
      "action": {
        "from": "0x0",
        "to": "0x0"
      }
    }]
    transactions = [{"_id": "0x1", "_source": {"from": "0x0", "to": "0x0"}}]
    self.internal_transactions._classify_trace(transactions, trace)
    assert "class" not in trace[0].keys()
    assert "class" in trace[1].keys()

  def test_classify_trace_partially(self):
    trace = [{
      "transactionHash": "0x2",
      "action": {
        "from": "0x0",
        "to": "0x0"
      }
    }, {
      "transactionHash": "0x1",
      "action": {
        "from": "0x0",
        "to": "0x0"
      }
    }]
    transactions = [{"_id": "0x1", "_source": {"from": "0x0", "to": "0x0"}}]
    self.internal_transactions._classify_trace(transactions, trace)
    assert "class" not in trace[0].keys()
    assert "class" in trace[1].keys()

  def test_set_block_number(self):
    trace = [{}, {}, {}]
    self.internal_transactions._set_block_number(trace, 123)
    for transaction in trace:
      assert transaction["blockNumber"] == 123

  def test_save_traces(self):
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {"blockNumber": 123}, id=1, refresh=True)
    self.internal_transactions._save_traces([123])
    transaction = self.client.get(TEST_TRANSACTIONS_INDEX, 'tx', 1)['_source']
    assert transaction['trace']

  def test_save_empty_traces(self):
    self.internal_transactions._save_traces([])
    assert True

  def test_preprocess_internal_transaction_with_empty_field(self):
    self.internal_transactions._preprocess_internal_transaction({"action": None})
    assert True

  def test_save_internal_transactions(self):
    test_trace = [{"transactionHash": "0x0"} for i in range(10)]
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

    internal_transactions = self.client.search(index=TEST_INTERNAL_TRANSACTIONS_INDEX, doc_type="itx", query="*")["hits"]["hits"]
    internal_transactions_bodies = [transaction["_source"] for transaction in internal_transactions]
    internal_transactions_ids = [transaction["_id"] for transaction in internal_transactions]
    self.assertCountEqual(internal_transactions_ids, test_transactions_ids)
    self.assertCountEqual(internal_transactions_bodies, test_transactions_bodies)

  def test_save_internal_transactions_ignore_rewards(self):
    trace = [{"transactionHash": None, "hash": "0x1"}]
    self.internal_transactions._save_internal_transactions(trace)
    internal_transactions = self.client.search(index=TEST_INTERNAL_TRANSACTIONS_INDEX, doc_type="itx", query="*")["hits"]["hits"]
    assert not len(internal_transactions)

  def test_save_miner_transaction(self):
    trace = [{"transactionHash": None, "hash": "0x1"}, {"transactionHash": "0x1"}]
    self.internal_transactions._save_miner_transactions(trace)
    miner_transactions = self.client.search(index=TEST_MINER_TRANSACTIONS_INDEX, doc_type="tx", query="*")["hits"]["hits"]
    assert len(miner_transactions) != 0
    assert miner_transactions[0]["_id"] == "0x1"
    self.assertCountEqual(miner_transactions[0]["_source"], {"transactionHash": None})

  def _add_transactions_and_return_chunk(self):
    docs = [{
      'hash': TEST_TRANSACTION_HASH,
      'from': TEST_TRANSACTION_HASH,
      'to': TEST_TRANSACTION_HASH,
      'to_contract': True,
      'id': i,
      'blockNumber': TEST_BLOCK_NUMBER,
      "transactionIndex": i % 10
    } for i in range(TEST_BIG_TRANSACTIONS_NUMBER)]
    self.client.bulk_index(TEST_TRANSACTIONS_INDEX, 'tx', docs, refresh=True)
    transactions = self.client.search(index=TEST_TRANSACTIONS_INDEX, doc_type='tx', query="*")['hits']['hits']
    return transactions, list(set([transaction["_source"]["blockNumber"] for transaction in transactions]))

  def test_extract_traces_chunk(self):
    test_blocks = ["0x{}".format(i) for i in range(10)]
    test_traces = {"0x{}".format(i): [{"transactionHash": "0x{}".format(i % 3)}] for i in range(10)}
    test_traces["0x0"].append({"transactionHash": None})
    test_transactions_chunks = [[str(j*10 + i) for i in range(10)] for j in range(10)]

    self.internal_transactions._get_traces = MagicMock(return_value=test_traces)
    self.internal_transactions._set_trace_hashes = MagicMock()
    self.internal_transactions._iterate_transactions = MagicMock(return_value=test_transactions_chunks)
    self.internal_transactions._classify_trace = MagicMock()
    self.internal_transactions._save_traces = MagicMock()
    self.internal_transactions._save_internal_transactions = MagicMock()
    self.internal_transactions._save_miner_transactions = MagicMock()
    process = Mock(
      get_traces=self.internal_transactions._get_traces,
      set_hashes=self.internal_transactions._set_trace_hashes,
      iterate=self.internal_transactions._iterate_transactions,
      classify=self.internal_transactions._classify_trace,
      save_traces=self.internal_transactions._save_traces,
      save_transactions=self.internal_transactions._save_internal_transactions,
      save_rewards=self.internal_transactions._save_miner_transactions
    )

    self.internal_transactions._extract_traces_chunk(test_blocks)

    calls = [call.get_traces(test_blocks)]
    for block, trace in test_traces.items():
      calls.append(call.set_hashes(trace))
      calls.append(call.iterate(block))
      for chunk in test_transactions_chunks:
        calls.append(call.classify(chunk, trace))
      calls += [call.save_transactions(trace), call.save_rewards(trace)]
    calls.append(call.save_traces(test_blocks))
    process.assert_has_calls(calls)

  def test_extract_traces_chunk_set_block_number(self):
    test_block = 123
    test_trace = [{}]
    self.internal_transactions._get_traces = MagicMock(return_value={test_block: test_trace})
    self.internal_transactions._iterate_transactions = MagicMock(return_value=[])
    self.internal_transactions._save_internal_transactions = MagicMock()
    self.internal_transactions._save_miner_transactions = MagicMock()
    self.internal_transactions._set_block_number = MagicMock()
    self.internal_transactions._set_trace_hashes = MagicMock()
    self.internal_transactions._save_traces = MagicMock()

    process = Mock(
      set_block=self.internal_transactions._set_block_number,
      save_transactions=self.internal_transactions._save_internal_transactions
    )

    self.internal_transactions._extract_traces_chunk([test_block])

    calls = [call.set_block(test_trace, test_block), call.save_transactions(test_trace)]
    process.assert_has_calls(calls)
    pass

  def test_extract_traces(self):
    test_blocks = list(range(10))
    test_chunks = [list(range(5)), list(range(5, 10))]
    self.internal_transactions._iterate_blocks = MagicMock(return_value=test_blocks)
    self.internal_transactions._split_on_chunks = MagicMock(return_value=test_chunks)
    self.internal_transactions._extract_traces_chunk = MagicMock()
    process = Mock(
      iterate=self.internal_transactions._iterate_blocks,
      split=self.internal_transactions._split_on_chunks,
      extract=self.internal_transactions._extract_traces_chunk
    )

    self.internal_transactions.extract_traces()

    calls = [call.iterate(), call.split(test_blocks, 10)]
    for chunk in test_chunks:
      calls.append(call.extract(chunk))
    process.assert_has_calls(calls)

  def test_process(self):
    test_transactions = self.client.search(index=REAL_TRANSACTIONS_INDEX, doc_type="tx", query="*", size=10000)['hits']['hits']
    test_transactions = [transaction["_source"] for transaction in test_transactions]
    test_blocks_number = len(list(set(transaction["blockNumber"] for transaction in test_transactions)))
    self.client.bulk_index(
      docs=test_transactions,
      index=TEST_TRANSACTIONS_INDEX,
      doc_type='tx',
      id_field="hash",
      refresh=True
    )

    self.internal_transactions.extract_traces()

    internal_transactions_count = self.client.count(index=TEST_INTERNAL_TRANSACTIONS_INDEX, doc_type="itx", query="*")["count"]
    miner_transactions_count = self.client.count(index=TEST_MINER_TRANSACTIONS_INDEX, doc_type="tx", query="*")["count"]
    assert internal_transactions_count > 0
    assert miner_transactions_count == test_blocks_number

REAL_TRANSACTIONS_INDEX = "ethereum-transaction"
TEST_TRANSACTIONS_NUMBER = 10
TEST_BLOCK_NUMBER = 3068185
TEST_BIG_TRANSACTIONS_NUMBER = TEST_TRANSACTIONS_NUMBER * 10
TEST_TRANSACTIONS_INDEX = 'test-ethereum-transactions'
TEST_INTERNAL_TRANSACTIONS_INDEX = 'test-ethereum-internal-transactions'
TEST_MINER_TRANSACTIONS_INDEX = 'test-ethereum-miner-transactions'
TEST_TRANSACTION_HASH = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_INPUT = '0xb1631db29e09ec5581a0ec398f1229abaf105d3524c49727621841af947bdc44'
TEST_INCORRECT_TRANSACTION_HASH = "0x"