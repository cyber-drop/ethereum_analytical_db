from pyelasticsearch import ElasticSearch
from internal_transactions import CustomElasticSearch as NewElasticSearch
import unittest
from time import time
import random

class ElasticSearchTestCase(unittest.TestCase):
  def setUp(self):
    self.client = ElasticSearch('http://localhost:9200')
    try:
      self.client.delete_index(TEST_INDEX)
    except:
      pass
    self.client.create_index(TEST_INDEX)
    self.new_client = NewElasticSearch('http://localhost:9200')

  def test_iterate_elasticsearch_data(self):
    print(self.new_client)
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
    print(len(list(items)))
    assert len(list(items)) == 100

  def unimplemented_test_pagination_without_scrolling(self):
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
