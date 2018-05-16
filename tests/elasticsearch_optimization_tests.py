import unittest
from test_utils import TestElasticSearch
from custom_elastic_search import CustomElasticSearch as NewElasticSearch
import json
import datetime as dt
from unittest.mock import MagicMock
import subprocess
from time import time

class ElasticSearchOptimizationTestCase():
  string_fields = []
  object_fields = []
  index_methods = [
    '_create_index_with_best_compression',
    '_set_max_result_size'
  ]
  mapping_methods = [
    '_set_object_properties_mapping',
    '_set_string_properties_mapping',
    '_disable_all_field'
  ]

  def setUp(self):
    self.client = TestElasticSearch()
    self.new_client = NewElasticSearch('http://localhost:9200')
    self.client.recreate_index(TEST_INDEX)

  def test_string_properties_mapping(self):
    self.new_client._set_string_properties_mapping(index=TEST_INDEX, doc_type=self.doc_type)
    mapping = self.client.get_mapping(index=TEST_INDEX)
    mapping_fields = mapping[TEST_INDEX]['mappings'][self.doc_type]['properties']
    for field in self.string_fields:
      mapping_field = mapping_fields[field]
      assert mapping_field["type"] == "keyword"
      assert not mapping_field["index"]

  def test_object_properties_mapping(self):
    self.new_client._set_object_properties_mapping(index=TEST_INDEX, doc_type=self.doc_type)
    mapping = self.client.get_mapping(index=TEST_INDEX)
    mapping_fields = mapping[TEST_INDEX]['mappings'][self.doc_type]['properties']
    for field in self.object_fields:
      mapping_field = mapping_fields[field]
      assert not mapping_field["enabled"]

  def test_disable_all_field(self):
    self.new_client._disable_all_field(index=TEST_INDEX, doc_type=self.doc_type)
    mapping = self.client.get_mapping(index=TEST_INDEX)
    all_field_mapping = mapping[TEST_INDEX]['mappings'][self.doc_type]['_all']
    assert all_field_mapping["enabled"] == False

  def test_create_index_with_best_compression(self):
    self.client.delete_index(TEST_INDEX)
    self.new_client._create_index_with_best_compression(TEST_INDEX)
    settings = self.client.get_settings(index=TEST_INDEX)[TEST_INDEX]["settings"]['index']
    assert settings["codec"] == "best_compression"

  def test_max_result_size(self):
    self.new_client._set_max_result_size(TEST_INDEX)
    settings = self.client.get_settings(index=TEST_INDEX)[TEST_INDEX]["settings"]['index']
    assert settings["max_result_window"] == '100000'

  def test_index_exists(self):
    index_exists = self.new_client._index_exists(TEST_INDEX)
    self.client.delete_index(TEST_INDEX)
    index_not_exists = self.new_client._index_exists(TEST_INDEX)
    assert index_exists
    assert not index_not_exists

  def _prepare_mock_methods(self):
    for method in self.index_methods + self.mapping_methods:
      setattr(self.new_client, method, MagicMock())

  def test_prepare_fast_index(self):
    self._prepare_mock_methods()
    self.new_client._index_exists = MagicMock(return_value=False)
    self.new_client.prepare_fast_index(TEST_INDEX, self.doc_type)
    self.new_client._index_exists.assert_called_with(TEST_INDEX)
    for method in self.index_methods:
      getattr(self.new_client, method).assert_called_with(TEST_INDEX)
    for method in self.mapping_methods:
      getattr(self.new_client, method).assert_called_with(TEST_INDEX, self.doc_type)

  def test_prepare_fast_non_empty_index(self):
    self._prepare_mock_methods()
    self.new_client._index_exists = MagicMock(return_value=True)
    self.new_client.prepare_fast_index(TEST_INDEX, self.doc_type)
    for method in self.index_methods + self.mapping_methods:
      getattr(self.new_client, method).assert_not_called()    

  def _get_elasticsearch_size(self):
    result = subprocess.run(["du", "-sb", "/var/lib/elasticsearch"], stdout=subprocess.PIPE)
    return int(result.stdout.split()[0])

  def _add_records(self, doc, number=10000, iterations=1):
    for _ in range(iterations):
      docs = [{**doc, **{"id": i + 1}} for i in range(0, number)]
      self.client.bulk_index(index=TEST_INDEX, doc_type=self.doc_type, docs=docs, refresh=True)

  def xtest_real_max_result_size(self):
    self.new_client._set_max_result_size(TEST_INDEX, 10)
    self._add_records({'test': 1}, number=10)
    with self.assertRaises(Exception):
      self._add_records({'test': 1}, number=1)

  def xtest_fast_index_size(self):
    self._add_records(self.doc)
    size_before = self._get_elasticsearch_size()

    self.client.delete_index(TEST_INDEX)
    self.new_client.prepare_fast_index(TEST_INDEX, doc_type=self.doc_type)
    self._add_records(self.doc)
    size_after = self._get_elasticsearch_size()    

    compression = size_after / size_before
    print("Compression: {:.1%}".format(compression))
    print("Current size: {:.1f}".format(CURRENT_ELASTICSEARCH_SIZE / (1024 ** 3)))
    print("Compressed size: {:.1f}".format(compression * CURRENT_ELASTICSEARCH_SIZE / (1024 ** 3)))
    assert size_after < size_before

  def xtest_fast_index_speed(self):
    start_time = time()
    self._add_records(self.doc)
    end_time = time()
    common_index_time = end_time - start_time

    self.client.delete_index(TEST_INDEX)
    self.new_client.prepare_fast_index(TEST_INDEX, doc_type=self.doc_type)

    start_time = time()
    self._add_records(self.doc)
    end_time = time()    
    fast_index_time = end_time - start_time

    boost = fast_index_time / common_index_time
    print("Time boost: {:.1%}".format(boost))
    assert fast_index_time < common_index_time

CURRENT_ELASTICSEARCH_SIZE = 290659165119
TEST_TRANSACTION = json.loads(
"""
{
  "blockNumber": 872857,
  "blockTimestamp": "2016-01-19T18:50:06",
  "from": "0x2ef08b6fd5616ef3771406f62f2e1615db9223dc",
  "hash": "0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e6",
  "input": "0x3f887fad000000000000000000000001878ace426dbfc40cf00c7479a1a544c3229531b700000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000015af1d78b58c400000000000000000000000000000000000000000000000000000000000000000000",
  "to": "0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085",
  "to_contract": true,
  "decoded_input": {
    "name": "sweep",
    "params": [
      {
        "type": "address",
        "value": "0x4156d3342d5c385a87d264f90653733592000581"
      },
      {
        "type": "uint256",
        "value": "30000000000"
      }
    ]
  },
  "trace": true,
  "value": 11.341286256167527
}
"""
)
TEST_INTERNAL_TRANSACTION = {
  'callType': 'call',
  'from': '0x7071f121c038a98f8a7d485648a27fcd48891ba8',
  'gas': '0x5208',
  'input': '0x',
  'to': '0xbff15491fdbb5a5dad14f7a5de7c4b994451c022',
  'value': '0xde3c6e090299400',
  'gasUsed': '0x0', 
  'output': '0x',
  'subtraces': 0,
  'traceAddress': [],
  'type': 'call'
}
TEST_TRANSACTION["blockTimestamp"] = dt.datetime.now()
TEST_INDEX = 'test-ethereum-transactions'

class TransactionsElasticSearchOptimizationTestCase(ElasticSearchOptimizationTestCase, unittest.TestCase):
  string_fields = ["from", "hash", "blockTimestamp"]
  object_fields = ["decoded_input"]
  doc_type = 'tx'
  doc = TEST_TRANSACTION

  def test_search_by_input_field(self):
    self.client.delete_index(TEST_INDEX)
    self.new_client.prepare_fast_index(TEST_INDEX, self.doc_type)
    self.client.index(
      index=TEST_INDEX, 
      doc_type=self.doc_type, 
      doc={'input': '0x0'}, 
      id=1,
      refresh=True
    )
    transactions = self.client.search(
      index=TEST_INDEX, 
      doc_type=self.doc_type, 
      query="input:0x?*"
    )['hits']['hits']
    assert len(transactions)

  def test_search_by_to_contract_field(self):
    self.client.delete_index(TEST_INDEX)
    self.new_client.prepare_fast_index(TEST_INDEX, self.doc_type)
    self.client.index(
      index=TEST_INDEX, 
      doc_type=self.doc_type, 
      doc={'to': "0x0"}, 
      id=1,
      refresh=True
    )

    transactions = self.client.search(
      index=TEST_INDEX, 
      doc_type=self.doc_type, 
      query={
        "query": {
          "terms": {
            "to": ["0x0"]
          }
        }
      }
    )['hits']['hits']
    assert len(transactions)    

class InternalTransactionsElasticSearchOptimizationTestCase(ElasticSearchOptimizationTestCase, unittest.TestCase):
  string_fields = ["callType", "from", "gas", "hash", "blockTimestamp", "gasUsed", "output"]
  object_fields = ["traceAddress"]
  doc_type = 'itx'
  doc = TEST_INTERNAL_TRANSACTION
