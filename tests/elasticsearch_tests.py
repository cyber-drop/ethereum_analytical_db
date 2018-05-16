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

  def xtest_pagination_without_scrolling(self):
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

TEST_INDEX = 'test-ethereum-transactions'
