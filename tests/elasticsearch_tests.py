from clients.custom_elastic_search import CustomElasticSearch as NewElasticSearch
import unittest
import random
from test_utils import TestElasticSearch


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

  def test_iterate_elasticsearch_data_with_pagination(self):
    for i in range(11):
      self.client.index(TEST_INDEX, 'item', {'paginate': True}, id=i + 1, refresh=True)
    iterator = self.new_client.iterate(index=TEST_INDEX, doc_type='item', query='paginate:true', per=10)
    items = next(iterator)
    item = next(iterator)
    assert len(items) == 10
    assert len(item) == 1

  def test_iterate_elasticsearch_data_with_object_query(self):
    for i in range(11):
      self.client.index(TEST_INDEX, 'item', {'paginate': True}, id=i + 1, refresh=True)
    iterator = self.new_client.iterate(index=TEST_INDEX, doc_type='item', query={"term": {"paginate": True}}, per=10)
    items = next(iterator)
    item = next(iterator)
    assert len(items) == 10
    assert len(item) == 1

  def test_deep_pagination(self):
    for i in range(100):
      self.client.index(TEST_INDEX, 'item', {'paginate': True}, id=i + 1, refresh=True)
    iterator = self.new_client.iterate(index=TEST_INDEX, doc_type='item', query='paginate:true', per=10)
    items = []
    for items_list in iterator:
      items.append(items_list)
      for j in range(20):
        self.client.update(TEST_INDEX, 'item', id=random.randint(1, 100), doc={'some_failing_flag': True})
    items = [i["_id"] for items_list in items for i in items_list]
    items = set(items)
    assert len(list(items)) == 100

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

  def test_send_sql_request(self):
    for i in range(100):
      self.client.index(TEST_INDEX, 'item', {'x': i}, id=i + 1, refresh=True)
    result = self.new_client.send_sql_request("SELECT max(x) FROM {}".format(TEST_INDEX))
    assert result == 99

TEST_INDEX = 'test-ethereum-transactions'
