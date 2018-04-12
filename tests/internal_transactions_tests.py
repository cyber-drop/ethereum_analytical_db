import unittest
from pyelasticsearch import ElasticSearch
from internal_transactions import InternalTransactions, elasticsearch_iterate
from time import sleep

class InternalTransactionsTestCase(unittest.TestCase):
  def setUp(self):
    self.client = ElasticSearch('http://localhost:9200')
    try:
      self.client.delete_index(TEST_INDEX)
    except:
      pass
    self.client.create_index(TEST_INDEX)
    self.internal_transactions = InternalTransactions(TEST_INDEX)

  def test_iterate_elasticsearch_data(self):
    for i in range(11):
      self.client.index(TEST_INDEX, 'item', {'paginate': True}, id=i + 1, refresh=True)
    iterator = elasticsearch_iterate(self.client, index=TEST_INDEX, doc_type='item', query='paginate:true', per=10)
    items = next(iterator)
    operations = [self.client.update_op(doc={'paginate': False}, id=i + 1) for i, item in enumerate(items)]
    self.client.bulk(operations, doc_type='item', index=TEST_INDEX, refresh=True)
    item = next(iterator)
    assert len(items) == 10
    assert len(item) == 1

  def test_iterate_transactions(self):
    self.client.index(TEST_INDEX, 'tx', {'to_contract': False}, id=1, refresh=True)
    self.client.index(TEST_INDEX, 'tx', {'to_contract': True, 'trace': {'test': 1}}, id=2, refresh=True)
    self.client.index(TEST_INDEX, 'tx', {'to_contract': True}, id=3, refresh=True)
    self.client.index(TEST_INDEX, 'nottx', {'to_contract': True}, id=4, refresh=True)
    iterator = self.internal_transactions._iterate_transactions()
    transactions = next(iterator)
    transactions = [transaction["_id"] for transaction in transactions]
    self.assertCountEqual(transactions, ['3'])

  def test_make_trace_requests(self):
    requests = self.internal_transactions._make_trace_requests({i: TEST_TRANSACTION_HASH for i in range(TEST_TRANSACTIONS_NUMBER)})
    assert len(requests) == TEST_TRANSACTIONS_NUMBER    
    for i, request in enumerate(requests):
      assert request["jsonrpc"] == "2.0"
      assert request["id"] == i
      assert request["method"] == "trace_replayTransaction"
      assert request["params"][0] == TEST_TRANSACTION_HASH
      assert request["params"][1][0] == "trace"

  def test_get_traces(self):
    traces = self.internal_transactions._get_traces({i: TEST_TRANSACTION_HASH for i in range(TEST_TRANSACTIONS_NUMBER)})
    for index, trace in traces.items():
      self.assertSequenceEqual(trace, TEST_TRANSACTION_TRACE)

  def test_save_traces(self):
    self.client.index(TEST_INDEX, 'tx', {"hash": TEST_TRANSACTION_HASH}, id=1, refresh=True)
    self.internal_transactions._save_traces({1: TEST_TRANSACTION_TRACE})
    transaction = self.client.get(TEST_INDEX, 'tx', 1)['_source']
    trace = transaction['trace']
    self.assertSequenceEqual(trace, TEST_TRANSACTION_TRACE)

  def test_extract_traces_chunk(self):
    docs = [{'to_contract': True, 'hash': TEST_TRANSACTION_HASH, 'id': i} for i in range(TEST_BIG_TRANSACTIONS_NUMBER)]
    self.client.bulk_index(TEST_INDEX, 'tx', docs, refresh=True)
    self.internal_transactions._extract_traces_chunk([{"_id": i, "_source": {"hash": TEST_TRANSACTION_HASH}} for i in range(TEST_TRANSACTIONS_NUMBER)])
    transactions = self.client.search("_exists_:trace", index=TEST_INDEX, doc_type='tx', size=TEST_TRANSACTIONS_NUMBER)['hits']['hits']
    transactions = [transaction["_id"] for transaction in transactions]
    self.assertCountEqual(transactions, [str(i) for i in range(TEST_TRANSACTIONS_NUMBER)])

  def test_extract_traces(self):
    docs = [{'to_contract': True, 'hash': TEST_TRANSACTION_HASH, 'id': i} for i in range(TEST_BIG_TRANSACTIONS_NUMBER)]
    self.client.bulk_index(TEST_INDEX, 'tx', docs, refresh=True)
    self.internal_transactions.extract_traces()
    transactions = self.client.search("_exists_:trace", index=TEST_INDEX, doc_type='tx', size=TEST_BIG_TRANSACTIONS_NUMBER)['hits']['hits']
    transactions = [transaction["_id"] for transaction in transactions]
    self.assertCountEqual(transactions, [str(i) for i in range(TEST_BIG_TRANSACTIONS_NUMBER)])

  def test_no_full_text_index(self):
    pass

TEST_TRANSACTIONS_NUMBER = 10
TEST_BIG_TRANSACTIONS_NUMBER = TEST_TRANSACTIONS_NUMBER * 10
TEST_INDEX = 'test-ethereum-transactions'
TEST_TRANSACTION_HASH = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_INPUT = '0xb1631db29e09ec5581a0ec398f1229abaf105d3524c49727621841af947bdc44'
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
  }
]