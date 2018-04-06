import unittest
from pyelasticsearch import ElasticSearch
from internal_transactions import InternalTransactions
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

  def test_transactions_to_contracts(self):
    self.client.index(TEST_INDEX, 'tx', {'input': '0x'}, id=1)
    self.client.index(TEST_INDEX, 'tx', {'input': TEST_TRANSACTION_INPUT}, id=2)
    self.client.index(TEST_INDEX, 'tx', {'input': None}, id=3)
    self.client.index(TEST_INDEX, 'nottx', {'input': TEST_TRANSACTION_INPUT}, id=4)
    sleep(1)
    transactions = self.internal_transactions.get_transactions_to_contracts()
    assert len(transactions) == 1
    assert transactions[0]['_id'] == '2'

  def test_make_trace_request(self):
    request = self.internal_transactions._make_trace_request(TEST_TRANSACTION_HASH)
    assert request["jsonrpc"] == "2.0"
    assert request["id"] == 1
    assert request["method"] == "trace_replayTransaction"
    assert request["params"][0] == TEST_TRANSACTION_HASH
    assert request["params"][1][0] == "trace"

  def test_get_trace(self):
    trace = self.internal_transactions.get_trace(TEST_TRANSACTION_HASH)
    self.assertSequenceEqual(trace, TEST_TRANSACTION_TRACE)

  def test_save_trace(self):
    self.client.index(TEST_INDEX, 'tx', {'input': TEST_TRANSACTION_INPUT, "hash": TEST_TRANSACTION_HASH}, id=1)
    self.internal_transactions.save_trace(1, TEST_TRANSACTION_TRACE)
    transaction = self.client.get(TEST_INDEX, 'tx', 1)['_source']
    trace = transaction['trace']
    self.assertSequenceEqual(trace, TEST_TRANSACTION_TRACE)

  def test_extract_traces(self):
    self.client.index(TEST_INDEX, 'tx', {'input': TEST_TRANSACTION_INPUT}, id=1)
    self.client.index(TEST_INDEX, 'tx', {'input': TEST_TRANSACTION_INPUT + 'a'}, id=2)
    self.internal_transactions.extract_traces([{"_id": '1', "_source": {"hash": TEST_TRANSACTION_HASH}}])
    transaction1 = self.client.get(TEST_INDEX, 'tx', 1)['_source']
    transaction2 = self.client.get(TEST_INDEX, 'tx', 2)['_source']
    self.assertSequenceEqual(transaction1['trace'], TEST_TRANSACTION_TRACE)
    assert 'trace' not in transaction2.keys()

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