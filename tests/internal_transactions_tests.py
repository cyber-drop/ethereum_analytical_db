import unittest
from test_utils import TestElasticSearch
from internal_transactions import *
from internal_transactions import _make_trace_requests, _get_parity_url_by_block, _get_traces_sync
import random
import requests
import json
from multiprocessing import Pool
from tqdm import *
import httpretty
from test_constants import TEST_BLOCK_TRACES

class InternalTransactionsTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_fast_index(TEST_TRANSACTIONS_INDEX)
    self.client.recreate_fast_index(TEST_INTERNAL_TRANSACTIONS_INDEX, doc_type='itx')
    self.client.recreate_index(TEST_BLOCKS_INDEX)
    self.parity_hosts = [(None, None, "http://localhost:8545")]
    self.internal_transactions = InternalTransactions({"block": TEST_BLOCKS_INDEX, "transaction": TEST_TRANSACTIONS_INDEX, "internal_transaction": TEST_INTERNAL_TRANSACTIONS_INDEX}, parity_hosts=self.parity_hosts)

  def test_split_on_chunks(self):
    test_list = list(range(10))
    test_chunks = list(self.internal_transactions._split_on_chunks(test_list, 3))
    self.assertSequenceEqual(test_chunks, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])

  def test_iterate_blocks(self):
    parity_hosts = [(0, 4, "http://localhost:8545"), (5, None, "http://localhost:8545")]
    self.internal_transactions = InternalTransactions({"block": TEST_BLOCKS_INDEX, "transaction": TEST_TRANSACTIONS_INDEX, "internal_transaction": TEST_INTERNAL_TRANSACTIONS_INDEX}, parity_hosts=parity_hosts)
    self.client.index(TEST_BLOCKS_INDEX, 'b', {'number': 1}, id=1, refresh=True)
    self.client.index(TEST_BLOCKS_INDEX, 'b', {'number': 2}, id=2, refresh=True)
    self.client.index(TEST_BLOCKS_INDEX, 'b', {'proceed': True, 'number': 3}, id=3, refresh=True)
    self.client.index(TEST_BLOCKS_INDEX, 'b', {'number': 4}, id=4, refresh=True)
    self.client.index(TEST_BLOCKS_INDEX, 'b', {'number': 5}, id=5, refresh=True)
    iterator = self.internal_transactions._iterate_blocks()
    blocks = next(iterator)
    blocks = [block["_id"] for block in blocks]
    self.assertCountEqual(blocks, ['1', '2', '5'])

  def test_iterate_transactions(self):
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to_contract': False, 'blockNumber': 1}, id=1, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to_contract': True, 'trace': {'test': 1}, 'blockNumber': 2}, id=2, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to_contract': True, 'blockNumber': 2}, id=3, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {'to_contract': True, 'blockNumber': 3}, id=4, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, 'nottx', {'to_contract': True, 'blockNumber': 1}, id=5, refresh=True)
    iterator = self.internal_transactions._iterate_transactions([1, 2])
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
      assert request["method"] == "trace_replayBlockTransactions"
      assert request["params"][0] == hex(TEST_BLOCK_NUMBER + i + 3)
      assert request["params"][1][0] == "trace"

  def test_get_traces(self):
    traces = self.internal_transactions._get_traces([TEST_BLOCK_NUMBER])
    self.assertSequenceEqual(traces[str(TEST_BLOCK_NUMBER)], TEST_BLOCK_TRACES)

  @httpretty.activate
  def test_get_traces_from_predefined_url(self):
    parity_hosts = [(10, 100, "http://localhost:8546/")]
    httpretty.register_uri(
      httpretty.POST, 
      "http://localhost:8546/", 
      body='[{"id": 90, "result": "test_traces"}]', 
      content_type='application/json'
    )
    self.internal_transactions = InternalTransactions(TEST_TRANSACTIONS_INDEX, parity_hosts=parity_hosts)
    traces = self.internal_transactions._get_traces([90])
    assert traces['90'] == "test_traces"

  def test_set_trace_hashes(self):
    transaction = {
      "hash": "0x1"
    }
    trace = [{}, {}, {}]
    self.internal_transactions._set_trace_hashes(transaction, trace)
    assert trace[0]["hash"] == "0x1.0"
    assert trace[1]["hash"] == "0x1.1"
    assert trace[2]["hash"] == "0x1.2"

  def test_classify_trace(self):
    trace = [{
      "action": {
        "from": "0x0",
        "to": "0x1"
      }
    }, {
      "action": {
        "from": "0x1",
        "to": "0x0"
      }
    }, {
      "action": {
        "from": "0x0",
        "to": "0x3"
      }
    }, {
      "action": {
        "from": "0x0",
        "to": "0x0"
      }
    }, {
      "action": {
        "from": "0x0"
      }
    }]
    transaction = {
      "from": "0x0",
      "to": "0x1"
    }
    self.internal_transactions._classify_trace(transaction, trace)
    assert trace[0]["class"] == INPUT_TRANSACTION
    assert trace[1]["class"] == INTERNAL_TRANSACTION
    assert trace[2]["class"] == OUTPUT_TRANSACTION
    assert trace[3]["class"] == OTHER_TRANSACTION
    assert trace[4]["class"] == OTHER_TRANSACTION

  def test_restore_transactions_dictionary(self):
    for i in range(5):
      self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {"blockNumber": TEST_BLOCK_NUMBER, "transactionIndex": i}, id=i + 1, refresh=True)
    transaction = self.client.search(index=TEST_TRANSACTIONS_INDEX, doc_type="tx", query="*")['hits']['hits']
    transactions_dict = self.internal_transactions._restore_transactions_dictionary({str(TEST_BLOCK_NUMBER): TEST_BLOCK_TRACES}, transaction)
    for i in range(5):
      transaction = transactions_dict[str(i + 1)]
      self.assertSequenceEqual(transaction, TEST_BLOCK_TRACES[i]['trace'])

  def test_save_traces(self):
    self.client.index(TEST_TRANSACTIONS_INDEX, 'tx', {"hash": TEST_TRANSACTION_HASH}, id=1, refresh=True)
    self.internal_transactions._save_traces({1: TEST_TRANSACTION_TRACE})
    transaction = self.client.get(TEST_TRANSACTIONS_INDEX, 'tx', 1)['_source']
    trace = transaction['trace']
    self.assertSequenceEqual(trace, TEST_TRANSACTION_TRACE)

  def test_save_empty_traces(self):
    self.internal_transactions._save_traces({})
    assert True

  def test_save_internal_transactions(self):
    traces = {1: TEST_TRANSACTION_TRACE}
    self.internal_transactions._save_internal_transactions(traces)
    internal_transactions = self.client.search(index=TEST_INTERNAL_TRANSACTIONS_INDEX, doc_type="itx", query="*")["hits"]["hits"]
    internal_transactions = [transaction["_source"] for transaction in internal_transactions]
    self.assertCountEqual(internal_transactions, TEST_INTERNAL_TRANSACTIONS)

  def test_save_internal_transactions_with_ids(self):
    trace = TEST_TRANSACTION_TRACE.copy()
    for i, transaction in enumerate(trace):
      transaction["hash"] = "0x1.{}".format(i)
    traces = {1: trace}
    self.internal_transactions._save_internal_transactions(traces)
    internal_transactions = self.client.search(index=TEST_INTERNAL_TRANSACTIONS_INDEX, doc_type="itx", query="*")["hits"]["hits"]
    internal_transactions = [transaction["_id"] for transaction in internal_transactions]
    self.assertCountEqual(internal_transactions, ["0x1.{}".format(i) for i, t in enumerate(trace)])

  def _add_transactions_and_return_chunk(self):
    docs = [{'to_contract': True, 'hash': TEST_TRANSACTION_HASH, 'to': TEST_TRANSACTION_HASH, 'from': TEST_TRANSACTION_HASH, 'id': i, 'blockNumber': TEST_BLOCK_NUMBER + i, "transactionIndex": 0} for i in range(TEST_BIG_TRANSACTIONS_NUMBER)]
    self.client.bulk_index(TEST_TRANSACTIONS_INDEX, 'tx', docs, refresh=True)
    transactions = self.client.search(index=TEST_TRANSACTIONS_INDEX, doc_type='tx', query="*")['hits']['hits']
    return transactions, [transaction["_source"]["blockNumber"] for transaction in transactions]

  def test_extract_traces_chunk(self):
    transactions_chunk, blocks_chunk = self._add_transactions_and_return_chunk()
    self.internal_transactions._extract_traces_chunk(blocks_chunk)
    transactions = self.client.search("_exists_:trace", index=TEST_TRANSACTIONS_INDEX, doc_type='tx', size=TEST_BIG_TRANSACTIONS_NUMBER)['hits']['hits']
    transactions = [transaction["_id"] for transaction in transactions]
    self.assertCountEqual(transactions, [t["_id"] for t in transactions_chunk])

  def test_extract_traces_chunk_with_preprocessing(self):
    transactions_chunk, blocks_chunk = self._add_transactions_and_return_chunk()
    self.internal_transactions._extract_traces_chunk(blocks_chunk)
    transactions = self.client.search("_exists_:trace", index=TEST_TRANSACTIONS_INDEX, doc_type='tx', size=TEST_TRANSACTIONS_NUMBER)['hits']['hits']
    transaction = transactions[0]["_source"]
    for internal_transaction in transaction["trace"]:
      assert 'hash' in internal_transaction.keys()
      assert 'class' in internal_transaction.keys()

  def test_extract_traces_chunk_with_internal_transactions(self):
    real_internal_transactions_length = sum([len(trace['trace']) for trace in TEST_BLOCK_TRACES])
    docs = [{'to_contract': True, 'hash': TEST_TRANSACTION_HASH + str(i), 'to': TEST_TRANSACTION_HASH, 'from': TEST_TRANSACTION_HASH, 'id': i, 'blockNumber': TEST_BLOCK_NUMBER, "transactionIndex": i} for i in range(len(TEST_BLOCK_TRACES))]
    self.client.bulk_index(TEST_TRANSACTIONS_INDEX, 'tx', docs, refresh=True)
    self.internal_transactions._extract_traces_chunk([TEST_BLOCK_NUMBER])
    internal_transactions = self.client.search("*", index=TEST_INTERNAL_TRANSACTIONS_INDEX, doc_type='itx', size=real_internal_transactions_length)['hits']['hits']
    assert len(internal_transactions) == real_internal_transactions_length

  def test_extract_traces(self):
    docs = [{'number': TEST_BLOCK_NUMBER + i, 'id': TEST_BLOCK_NUMBER + i} for i in range(5)]
    self.client.bulk_index(TEST_BLOCKS_INDEX, 'b', docs, refresh=True)
    docs = [{'to_contract': True, 'hash': TEST_TRANSACTION_HASH, 'id': i, 'blockNumber': TEST_BLOCK_NUMBER + (i % 5), 'transactionIndex': 0} for i in range(TEST_BIG_TRANSACTIONS_NUMBER)]
    self.client.bulk_index(TEST_TRANSACTIONS_INDEX, 'tx', docs, refresh=True)
    self.internal_transactions.extract_traces()
    transactions = self.client.search("_exists_:trace", index=TEST_TRANSACTIONS_INDEX, doc_type='tx', size=TEST_BIG_TRANSACTIONS_NUMBER)['hits']['hits']
    transactions = [transaction["_id"] for transaction in transactions]
    self.assertCountEqual(transactions, [str(i) for i in range(TEST_BIG_TRANSACTIONS_NUMBER)])

TEST_TRANSACTIONS_NUMBER = 10
TEST_BLOCK_NUMBER = 5411342
TEST_BIG_TRANSACTIONS_NUMBER = TEST_TRANSACTIONS_NUMBER * 10
TEST_TRANSACTIONS_INDEX = 'test-ethereum-transactions'
TEST_BLOCKS_INDEX = 'test-ethereum-blocks'
TEST_INTERNAL_TRANSACTIONS_INDEX = 'test-ethereum-internal-transactions'
TEST_TRANSACTION_HASH = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_INPUT = '0xb1631db29e09ec5581a0ec398f1229abaf105d3524c49727621841af947bdc44'
TEST_INCORRECT_TRANSACTION_HASH = "0x"
TEST_TRANSACTION_TRACE = [
  {
    "action": {
      "callType": "call",
      "from": "0xa74d69c0aef9166aca23d563f38cbf85fe3e39a6",
      "gas": "0x104f8",
      "input": "0x3cc86b80000000000000000000000000000000000000000000000000016345785d8a0000000000000000000000000000a74d69c0aef9166aca23d563f38cbf85fe3e39a6",
      "to": "0x1fcb809dbe044fb3875463281d1bb55c4476a28b",
      "value": "0x0"
    },
    "result": {
      "gasUsed": "0x1bbd",
      "output": "0x"
    },
    "subtraces": 1,
    "traceAddress": [],
    "type": "call"
  },
  {
    "action": {
      "callType": "call",
      "from": "0x1fcb809dbe044fb3875463281d1bb55c4476a28b",
      "gas": "0x8fc",
      "input": "0x",
      "to": "0xa74d69c0aef9166aca23d563f38cbf85fe3e39a6",
      "value": "0x16345785d8a0000"
    },
    "result": {
      "gasUsed": "0x0",
      "output": "0x"
    },
    "subtraces": 0,
    "traceAddress": [
      0
    ],
    "type": "call"
  },
  {
    'subtraces': 0, 
    'action': {
      'to': '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0', 
      'from': '0xce6cd85aabf4a5a9a3f4c85381f9e47f957940b2', 
      'callType': 'call', 
      'gas': '0x234e8', 
      'value': '0x0', 
      'input': '0xa9059cbb0000000000000000000000006cc5f688a315f3dc28a7781717a9a798a59fda7b0000000000000000000000000000000000000000000000838a8ebe0301a4ffff'
    }, 
    'type': 'call', 
    'error': 'Bad instruction', 
    'hash': '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a.0', 
    'traceAddress': []
  }
]
TEST_INTERNAL_TRANSACTIONS = [
  {
    "callType": "call",
    "from": "0xa74d69c0aef9166aca23d563f38cbf85fe3e39a6",
    "gas": "0x104f8",
    "input": "0x3cc86b80000000000000000000000000000000000000000000000000016345785d8a0000000000000000000000000000a74d69c0aef9166aca23d563f38cbf85fe3e39a6",
    "to": "0x1fcb809dbe044fb3875463281d1bb55c4476a28b",
    "value": "0x0",
    "gasUsed": "0x1bbd",
    "output": "0x",
    "subtraces": 1,
    "traceAddress": [],
    "type": "call"
  }, 
  {
    "callType": "call",
    "from": "0x1fcb809dbe044fb3875463281d1bb55c4476a28b",
    "gas": "0x8fc",
    "input": "0x",
    "to": "0xa74d69c0aef9166aca23d563f38cbf85fe3e39a6",
    "value": "0x16345785d8a0000",
    "gasUsed": "0x0",
    "output": "0x",
    "subtraces": 0,
    "traceAddress": [0],
    "type": "call"
  },
  {
    'subtraces': 0, 
    'to': '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0', 
    'from': '0xce6cd85aabf4a5a9a3f4c85381f9e47f957940b2', 
    'callType': 'call', 
    'gas': '0x234e8', 
    'value': '0x0', 
    'input': '0xa9059cbb0000000000000000000000006cc5f688a315f3dc28a7781717a9a798a59fda7b0000000000000000000000000000000000000000000000838a8ebe0301a4ffff',
    'type': 'call', 
    'error': 'Bad instruction', 
    'traceAddress': []
  }
]

