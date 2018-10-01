import unittest
from tests.test_utils import TestElasticSearch
from operations.indices import ElasticSearchIndices
from unittest.mock import MagicMock
import subprocess
from time import time
from tests.test_utils import mockify

class ElasticSearchIndicesTestCase(unittest.TestCase):
  string_fields = ["callType", "from", "gas", "hash", "blockTimestamp", "gasUsed",
                   "blockHash", "transactionHash", "refundAddress", "to",
                   "type", "address", "balance", "blockNumber"]
  text_fields = ["input", "code", "init", "error", "output"]
  object_fields = ["traceAddress", "decoded_input"]
  doc_type = 'itx'
  doc = {
    "input": "0x000000000000000000000000000000000000000000000000000000000032a44c",
    "type": "call",
    "output": "0x0000000000000000000000000000000000000000000000000000000000000001",
    "subtraces": 1,
    "gasUsed": "0x6",
    "from": "0x6a0a0fc761c612c340a0e98d33b37a75e5268472",
    "gas": "0x6",
    "value": 0,
    "class": 3,
    "to": "0x0f045b8a7f4587cdff0919fe8d12613a7e1b7230",
    "callType": "call",
    "blockHash": "0x31864a7a7ed528fe40156126c52fdcc8cdefaf692a92e8b394e108df91dbe106",
    "transactionHash": "0xd05ab992923b075bbf4b6be784cc4b386d3acaf41af9760d0465eff8499d3b3e",
    "refundAddress": "0x12ef7e5ff5693849fcbb7e06e6376686b4499ffd",
    "code": "0x606060405263ffffffff60e060020a6000350416636ea056a98114610021575bfe5b341561002957fe5b610040600160a060020a0360043516602435610054565b604080519115158252519081900360200190f35b6000805460408051602090810184905281517f3c18d318000000000000000000000000000000000000000000000000000000008152600160a060020a03878116600483015292519290931692633c18d318926024808301939282900301818787803b15156100be57fe5b60325a03f115156100cb57fe5b50505060405180519050600160a060020a0316600036600060405160200152604051808383808284378201915050925050506020604051808303818560325a03f4151561011457fe5b50506040515190505b929150505600a165627a7a7230582072faa239cc9c48e881b02f074d012a710ff574cc3be6ae9a976f28ad2aaaf6710029",
    "init": "0x6060604052341561000c57fe5b60405160208061019a83398101604052515b60008054600160a060020a031916600160a060020a0383161790555b505b61014f8061004b6000396000f300606060405263ffffffff60e060020a6000350416636ea056a98114610021575bfe5b341561002957fe5b610040600160a060020a0360043516602435610054565b604080519115158252519081900360200190f35b6000805460408051602090810184905281517f3c18d318000000000000000000000000000000000000000000000000000000008152600160a060020a03878116600483015292519290931692633c18d318926024808301939282900301818787803b15156100be57fe5b60325a03f115156100cb57fe5b50505060405180519050600160a060020a0316600036600060405160200152604051808383808284378201915050925050506020604051808303818560325a03f4151561011457fe5b50506040515190505b929150505600a165627a7a7230582072faa239cc9c48e881b02f074d012a710ff574cc3be6ae9a976f28ad2aaaf67100290000000000000000000000004f01001cf69785d4c37f03fd87398849411ccbba",
    "error": "Out of gas",
    "traceAddress": [1, 0],
    "decoded_input": {
      "name": "approve",
      "params": [
        {
          "type": "address",
          "value": "0x8d12a197cb00d4747a1fe03395095ce2a5cc6819"
        },
        {
          "type": "uint256",
          "value": "50000000000000000000000000"
        }
      ]
    }
  }
  index_methods = [
    '_create_index_with_best_compression',
    '_set_max_result_size'
  ]
  mapping_methods = [
    '_set_object_properties_mapping',
    '_set_string_properties_mapping',
    '_set_text_properties_mapping',
    '_disable_all_field'
  ]

  def setUp(self):
    self.client = TestElasticSearch()
    self.prepare_indices = ElasticSearchIndices(TEST_INDICES)
    self.client.recreate_index(TEST_INDEX)

  def test_string_properties_mapping(self):
    self.prepare_indices._set_string_properties_mapping(index=TEST_INDEX, doc_type=self.doc_type)
    mapping = self.client.get_mapping(index=TEST_INDEX)
    mapping_fields = mapping[TEST_INDEX]['mappings'][self.doc_type]['properties']
    for field in self.string_fields:
      mapping_field = mapping_fields[field]
      assert mapping_field["type"] == "keyword"

  def test_text_properties_mapping(self):
    self.prepare_indices._set_text_properties_mapping(index=TEST_INDEX, doc_type=self.doc_type)
    mapping = self.client.get_mapping(index=TEST_INDEX)
    mapping_fields = mapping[TEST_INDEX]['mappings'][self.doc_type]['properties']
    for field in self.text_fields:
      mapping_field = mapping_fields[field]
      assert mapping_field["type"] == "text"
      assert not mapping_field["index"]
      assert mapping_field['fields']["keyword"]["ignore_above"] > 100

  def test_object_properties_mapping(self):
    self.prepare_indices._set_object_properties_mapping(index=TEST_INDEX, doc_type=self.doc_type)
    mapping = self.client.get_mapping(index=TEST_INDEX)
    mapping_fields = mapping[TEST_INDEX]['mappings'][self.doc_type]['properties']
    for field in self.object_fields:
      mapping_field = mapping_fields[field]
      assert not mapping_field["enabled"]

  def test_disable_all_field(self):
    self.prepare_indices._disable_all_field(index=TEST_INDEX, doc_type=self.doc_type)
    mapping = self.client.get_mapping(index=TEST_INDEX)
    all_field_mapping = mapping[TEST_INDEX]['mappings'][self.doc_type]['_all']
    assert all_field_mapping["enabled"] == False

  def test_create_index_with_best_compression(self):
    self.client.delete_index(TEST_INDEX)
    self.prepare_indices._create_index_with_best_compression(TEST_INDEX)
    settings = self.client.get_settings(index=TEST_INDEX)[TEST_INDEX]["settings"]['index']
    assert settings["codec"] == "best_compression"

  def test_max_result_size(self):
    self.prepare_indices._set_max_result_size(TEST_INDEX)
    settings = self.client.get_settings(index=TEST_INDEX)[TEST_INDEX]["settings"]['index']
    assert settings["max_result_window"] == '100000'

  def test_index_exists(self):
    index_exists = self.prepare_indices._index_exists(TEST_INDEX)
    self.client.delete_index(TEST_INDEX)
    index_not_exists = self.prepare_indices._index_exists(TEST_INDEX)
    assert index_exists
    assert not index_not_exists

  def test_prepare_fast_index(self):
    mockify(self.prepare_indices, {
      "_index_exists": MagicMock(return_value=False)
    }, "_prepare_fast_index")

    self.prepare_indices._prepare_fast_index(TEST_INDEX, self.doc_type)

    self.prepare_indices._index_exists.assert_called_with(TEST_INDEX)
    for method in self.index_methods:
      getattr(self.prepare_indices, method).assert_called_with(TEST_INDEX)
    for method in self.mapping_methods:
      getattr(self.prepare_indices, method).assert_called_with(TEST_INDEX, self.doc_type)

  def test_prepare_fast_non_empty_index(self):
    mockify(self.prepare_indices, {
      "_index_exists": MagicMock(return_value=True)
    }, "_prepare_fast_index")
    self.prepare_indices._prepare_fast_index(TEST_INDEX, self.doc_type)
    for method in self.index_methods + self.mapping_methods:
      getattr(self.prepare_indices, method).assert_not_called()

  def test_prepare_index(self):
    self.prepare_indices._index_exists = MagicMock(return_value=False)
    self.prepare_indices.client.create_index = MagicMock()

    self.prepare_indices._prepare_index(TEST_INDEX)

    self.prepare_indices._index_exists.assert_called_with(TEST_INDEX)
    self.prepare_indices.client.create_index.assert_called_with(TEST_INDEX)

  def test_prepare_non_empty_index(self):
    self.prepare_indices._index_exists = MagicMock(return_value=True)
    self.prepare_indices.client.create_index = MagicMock()

    self.prepare_indices._prepare_index(TEST_INDEX)

    self.prepare_indices.client.create_index.assert_not_called()

  def test_prepare_indices(self):
    self.prepare_indices._prepare_index = MagicMock()
    self.prepare_indices._prepare_fast_index = MagicMock()

    self.prepare_indices.prepare_indices()

    self.prepare_indices._prepare_fast_index.assert_any_call(TEST_INDICES["internal_transaction"], "itx")
    for key in TEST_INDICES:
      if key != "internal_transaction":
        self.prepare_indices._prepare_index.assert_any_call(TEST_INDICES[key])


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
TEST_INDEX = 'test-ethereum-transactions'
TEST_INDICES = {
  "contract": "test-ethereum-contract",
  "transaction": "test-ethereum-transaction",
  "internal_transaction": "test-ethereum-internal-transaction",
  "listed_token": "test-ethereum-listed-token",
  "token_tx": "test-ethereum-token-transaction",
  "block": "test-ethereum-block",
  "miner_transaction": "test-ethereum-miner-transaction",
  "token_price": "test-ethereum-token-price"
}

class ClickhouseIndicesTestCase(unittest.TestCase):
  pass