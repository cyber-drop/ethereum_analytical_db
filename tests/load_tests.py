import unittest
from pyelasticsearch import ElasticSearch
from internal_transactions import *
from internal_transactions import _make_trace_requests, _get_parity_url_by_block
from time import sleep, time
import random
import requests
import json
from multiprocessing import Pool
from tqdm import *

class SergeImplementation():
  def _http_post_request(self, url, request):
    return requests.post(url, data=request, headers={"content-type": "application/json"}).json()

  def _make_request_trace(self, hash):
    return json.dumps({
        "jsonrpc": "2.0",
        "method": "trace_replayTransaction",
        "params": [hash,
                   ["trace"]],
        "id": 1
    })

  def _get_trace(self, hash):
    self._http_post_request(
      'http://localhost:8545',
      self._make_request_trace(hash))['result']['trace']

  def get_traces(self, hashes):
    pool = Pool(processes=10)
    return pool.map(self._get_trace, hashes)

class LoadTestCase(unittest.TestCase):
  def setUp(self):
    self.client = ElasticSearch('http://localhost:9200')
    try:
      self.client.delete_index(TEST_INDEX)
    except:
      pass
    self.client.create_index(TEST_INDEX)
    self.internal_transactions = InternalTransactions(TEST_INDEX)
    del PARITY_HOSTS[:]
    PARITY_HOSTS.append((None, None, "http://localhost:8545"))

  def test_get_traces_faster_than_serge(self):
    real_client = ElasticSearch('http://localhost:9200')
    attemps = []
    real_hashes = real_client.search(index=INDEX_WITH_REAL_DATA, doc_type="tx", query='to_contract:true', size=TEST_BIG_TRANSACTIONS_NUMBER*TEST_ATTEMPS*3)['hits']['hits']
    real_hashes = [t["_source"]["hash"] for t in real_hashes]
    serge_implementation = SergeImplementation()
    for attemp in tqdm(range(0, TEST_ATTEMPS*3, 3)):
      start_serge_chunk = attemp * TEST_BIG_TRANSACTIONS_NUMBER
      end_serge_chunk = (attemp + 1) * TEST_BIG_TRANSACTIONS_NUMBER
      start_my_chunk = (attemp + 1) * TEST_BIG_TRANSACTIONS_NUMBER
      end_my_chunk = (attemp + 2) * TEST_BIG_TRANSACTIONS_NUMBER
      start_time_for_serge_extractor = time()
      response = serge_implementation.get_traces(real_hashes[start_serge_chunk:end_serge_chunk])
      assert len(response) == TEST_BIG_TRANSACTIONS_NUMBER
      start_time_for_my_extractor = time()
      response = self.internal_transactions._get_traces({i: hash for i, hash in enumerate(real_hashes[start_my_chunk:end_my_chunk])})
      assert len(response.keys()) == TEST_BIG_TRANSACTIONS_NUMBER
      end_time = time()
      serge_time = start_time_for_my_extractor - start_time_for_serge_extractor
      my_time = end_time - start_time_for_my_extractor
      attemps.append(my_time < serge_time)
      print(my_time, serge_time)
    assert all(attemps)

TEST_TRANSACTIONS_NUMBER = 10
TEST_TRANSACTION_HASH = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_BIG_TRANSACTIONS_NUMBER = TEST_TRANSACTIONS_NUMBER * 10
TEST_INDEX = 'test-ethereum-transactions'
INDEX_WITH_REAL_DATA = "ethereum-transaction"
TEST_ATTEMPS = 5
