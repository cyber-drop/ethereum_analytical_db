from pyelasticsearch import ElasticSearch
from custom_elastic_search import CustomElasticSearch as NewElasticSearch
import unittest
from time import time
import random
from test_utils import TestElasticSearch
import json
import datetime as dt
import subprocess
from tqdm import tqdm

CURRENT_ELASTICSEARCH_SIZE = 290659165119

class ElasticSearchTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.new_client = NewElasticSearch('http://localhost:9200')
    self.client.recreate_index(TEST_INDEX)

  def test_make_range_query(self):
    assert self.new_client.make_range_query("block", (0, 3)) == "block:[0 TO 2]"
    assert self.new_client.make_range_query("block", (None, 3)) == "block:[* TO 2]"
    assert self.new_client.make_range_query("block", (0, None)) == "block:[0 TO *]"
    assert self.new_client.make_range_query("block", (None, None)) == "block:[* TO *]"

  def test_make_complex_range_query(self):
    assert self.new_client.make_range_query("block", (0, 3), (10, 100)) == "(block:[0 TO 2] OR block:[10 TO 99])"

  def test_iterate_elasticsearch_data(self):
    for i in range(11):
      self.client.index(TEST_INDEX, 'item', {'paginate': True}, id=i + 1, refresh=True)
    iterator = self.new_client.iterate(index=TEST_INDEX, doc_type='item', query='paginate:true', per=10)
    items = next(iterator)
    operations = [self.client.update_op(doc={'paginate': False}, id=i + 1) for i, item in enumerate(items)]
    self.client.bulk(operations, doc_type='item', index=TEST_INDEX, refresh=True)
    item = next(iterator)
    assert len(items) == 10
    assert len(item) == 1

  def test_iterate_elasticsearch_data_with_pagination(self):
    for i in range(11):
      self.client.index(TEST_INDEX, 'item', {'paginate': True}, id=i + 1, refresh=True)
    iterator = self.new_client.iterate(index=TEST_INDEX, doc_type='item', query='paginate:true', per=10, paginate=True)
    items = next(iterator)
    item = next(iterator)
    assert len(items) == 10
    assert len(item) == 1

  def test_iterate_elasticsearch_data_with_object_query(self):
    for i in range(11):
      self.client.index(TEST_INDEX, 'item', {'paginate': True}, id=i + 1, refresh=True)
    iterator = self.new_client.iterate(index=TEST_INDEX, doc_type='item', query={"term": {"paginate": True}}, per=10, paginate=True)
    items = next(iterator)
    item = next(iterator)
    assert len(items) == 10
    assert len(item) == 1

  def test_deep_pagination(self):
    for i in range(100):
      self.client.index(TEST_INDEX, 'item', {'paginate': True}, id=i + 1, refresh=True)
    iterator = self.new_client.iterate(index=TEST_INDEX, doc_type='item', query='paginate:true', per=10, paginate=True)
    items = []
    for items_list in iterator:
      items.append(items_list)
      for j in range(20):
        self.client.update(TEST_INDEX, 'item', id=random.randint(1, 100), doc={'some_failing_flag': True})
    items = [i["_id"] for items_list in items for i in items_list]
    items = set(items)
    assert len(list(items)) == 100

  def unimplemented_pagination_without_scrolling(self):
    docs = [{'paginate': True, 'id': i + 1} for i in range(1000)]
    self.client.bulk_index(docs=docs, doc_type='item', index=TEST_INDEX, refresh=True)
    attemps = []
    for attemp in range(10):
      time_start_scrolling = time()    
      iterator = self.new_client.iterate(index=TEST_INDEX, doc_type='item', query='paginate:true', per=10, paginate=True, scrolling=True)
      for transactions in iterator:
        pass
      time_start_pagination = time()
      iterator = self.new_client.iterate(index=TEST_INDEX, doc_type='item', query='paginate:true', per=10, paginate=True, scrolling=False)
      for transactions in iterator:
        pass
      time_end = time()
      print("Pagination time: ", time_end - time_start_pagination)
      print("Scrolling time: ", time_start_pagination - time_start_scrolling)
      attemps.append((time_end - time_start_pagination) < (time_start_pagination - time_start_scrolling))
    print(attemps)
    assert all(attemps)

  def test_elasticsearch_update_by_query(self):
    self.add_transactions_for_update()
    self.new_client.update_by_query(TEST_INDEX, 'item', 'will_update:true', 'ctx._source.updated = true')
    updated_records = self.client.search("updated:true", index=TEST_INDEX, doc_type='item')['hits']['hits']
    updated_records = [record["_id"] for record in updated_records]
    self.assertCountEqual(updated_records, [str(i + 1) for i in range(5)])

  def test_elasticsearch_update_by_query_object(self):
    self.add_transactions_for_update()
    self.new_client.update_by_query(TEST_INDEX, 'item', {'term': {'will_update': True}}, 'ctx._source.updated = true')
    updated_records = self.client.search("updated:true", index=TEST_INDEX, doc_type='item')['hits']['hits']
    updated_records = [record["_id"] for record in updated_records]
    self.assertCountEqual(updated_records, [str(i + 1) for i in range(5)])

  def add_transactions_for_update(self):
    for i in range(5):
      self.client.index(TEST_INDEX, 'item', {'will_update': True}, id=i + 1, refresh=True)
    for i in range(5):
      self.client.index(TEST_INDEX, 'item', {'will_update': False}, id=i + 6, refresh=True)

  def _get_elasticsearch_size(self):
    result = subprocess.run(["du", "-sb", "/var/lib/elasticsearch"], stdout=subprocess.PIPE)
    return int(result.stdout.split()[0])

  def _add_records(self, doc, number=10000, iterations=1):
    for _ in range(iterations):
      docs = [{**doc, **{"id": i + 1}} for i in range(0, number)]
      self.client.bulk_index(index=TEST_INDEX, doc_type='tx', docs=docs, refresh=True)

  def test_max_result_size(self):
    self.client.delete_index(TEST_INDEX)
    self.new_client.prepare_fast_index(TEST_INDEX)
    self._add_records({'test': 1}, number=100000)    

  def test_final_index_size(self):
    self._add_records(TEST_TRANSACTION)
    size_before = self._get_elasticsearch_size()

    self.client.delete_index(TEST_INDEX)
    self.new_client.prepare_fast_index(TEST_INDEX)
    self._add_records(TEST_TRANSACTION)
    size_after = self._get_elasticsearch_size()    

    compression = size_after / size_before
    print("Compression: {:.1%}".format(compression))
    print("Current size: {:.1f}".format(CURRENT_ELASTICSEARCH_SIZE / (1024 ** 3)))
    print("Compressed size: {:.1f}".format(compression * CURRENT_ELASTICSEARCH_SIZE / (1024 ** 3)))
    assert size_after < size_before

  def test_search_by_non_indexed_fields(self):
    self.client.delete_index(TEST_INDEX)
    self.new_client.prepare_fast_index(TEST_INDEX)
    self._add_records(TEST_TRANSACTION, number=100)
    for property in ["decoded_input", "trace"]:
      object_search_result = self.client.search(index=TEST_INDEX, doc_type='tx', query="_exists_:{}".format(property))['hits']['hits']
      assert len(object_search_result)

  def test_index_size_depending_on_records_number(self):
    x = list(range(10))
    no_compression_y = []
    compression_y = []
    for i in tqdm(x):
      self.client.recreate_index(TEST_INDEX)
      self._add_records(TEST_TRANSACTION, 1000, i)
      no_compression_y.append(self._get_elasticsearch_size())
    for i in tqdm(x):
      self.client.delete_index(TEST_INDEX)
      self.new_client.prepare_fast_index(TEST_INDEX)
      self._add_records(TEST_TRANSACTION, 1000, i)
      compression_y.append(self._get_elasticsearch_size())

    plt.plot(x, no_compression_y)
    plt.plot(x, compression_y)
    plt.show()

  def test_non_empty_index(self):
    self._add_records(TEST_TRANSACTION, number=100)
    size_before = self._get_elasticsearch_size()
    self.new_client.prepare_fast_index(TEST_INDEX)
    size_after = self._get_elasticsearch_size()
    assert size_after == size_before

  def test_index_exists(self):
    index_exists = self.new_client._index_exists(TEST_INDEX)
    self.client.delete_index(TEST_INDEX)
    index_not_exists = self.new_client._index_exists(TEST_INDEX)
    assert index_exists
    assert not index_not_exists

TEST_INDEX = 'test-ethereum-transactions'
TEST_TRANSACTION = json.loads('{"blockNumber":872857,"blockTimestamp":"2016-01-19T18:50:06","from":"0x2ef08b6fd5616ef3771406f62f2e1615db9223dc","hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e6","input":"0x3f887fad000000000000000000000001878ace426dbfc40cf00c7479a1a544c3229531b700000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000015af1d78b58c400000000000000000000000000000000000000000000000000000000000000000000","to":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","to_contract":true,"trace":[{"action":{"callType":"call","from":"0x2ef08b6fd5616ef3771406f62f2e1615db9223dc","gas":"0x2d6a68","input":"0x3f887fad000000000000000000000001878ace426dbfc40cf00c7479a1a544c3229531b700000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000015af1d78b58c400000000000000000000000000000000000000000000000000000000000000000000","to":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","value":"0x9d6457c2e2e29ecd"},"class":0,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e60","result":{"gasUsed":"0xe53a","output":"0x"},"subtraces":10,"traceAddress":[],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2d0381","input":"0x24d4e90a0000000000000000000000000000000000000000000000030000000000000000","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e61","result":{"gasUsed":"0x9e0","output":"0x000000000000000000000000000000000000000000000001193ea7aad0311384"},"subtraces":0,"traceAddress":[0],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2ced85","input":"0x4b09ebb20000000000000000000000000000000000000000000000005c878eb6eae51aa0","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e62","result":{"gasUsed":"0x2da","output":"0x0000000000000000000000000000000000000000000000016f76596861d8b7f9"},"subtraces":0,"traceAddress":[1],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2ce83c","input":"0x4b09ebb20000000000000000000000000000000000000000000000000e0feec88a68d940","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e63","result":{"gasUsed":"0x2da","output":"0x0000000000000000000000000000000000000000000000010e74a452d439ed42"},"subtraces":0,"traceAddress":[2],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2ce2f3","input":"0x4b09ebb20000000000000000000000000000000000000000000000000000000000000000","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e64","result":{"gasUsed":"0x2da","output":"0x00000000000000000000000000000000000000000000000100000016aee6e8ef"},"subtraces":0,"traceAddress":[3],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2cde62","input":"0x24d4e90a0000000000000000000000000000000000000000000000037deafdd1e4f98e2a","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e65","result":{"gasUsed":"0x9e0","output":"0x000000000000000000000000000000000000000000000001401c9bc97658c7c2"},"subtraces":0,"traceAddress":[4],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2cbcee","input":"0x4b09ebb2000000000000000000000000000000000000000000000000a2d738a19ef158e0","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e66","result":{"gasUsed":"0x2da","output":"0x000000000000000000000000000000000000000000000001e39ac8cf61d61e8f"},"subtraces":0,"traceAddress":[5],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2cb7a5","input":"0x4b09ebb20000000000000000000000000000000000000000000000000e0feec88a68d940","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e67","result":{"gasUsed":"0x2da","output":"0x0000000000000000000000000000000000000000000000010e74a452d439ed42"},"subtraces":0,"traceAddress":[6],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2cb25c","input":"0x4b09ebb20000000000000000000000000000000000000000000000000000000000000000","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e68","result":{"gasUsed":"0x2da","output":"0x00000000000000000000000000000000000000000000000100000016aee6e8ef"},"subtraces":0,"traceAddress":[7],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2cadcb","input":"0x24d4e90a000000000000000000000000000000000000000000000003f20f6d38e4f6f4c0","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e69","result":{"gasUsed":"0x9e0","output":"0x0000000000000000000000000000000000000000000000015f61ea75151f9cf5"},"subtraces":0,"traceAddress":[8],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x0","input":"0x","to":"0x2ef08b6fd5616ef3771406f62f2e1615db9223dc","value":"0x0"},"class":1,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e610","result":{"gasUsed":"0x0","output":"0x"},"subtraces":0,"traceAddress":[9],"type":"call"}],"value":11.341286256167527}')
TEST_TRANSACTION["blockTimestamp"] = dt.datetime.now()
TEST_TRANSACTION["decoded_input"] = json.loads('{"name":"sweep","params":[{"type":"address","value":"0x4156d3342d5c385a87d264f90653733592000581"},{"type":"uint256","value":"30000000000"}]}')