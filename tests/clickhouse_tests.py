import unittest
from clickhouse_driver import Client
from clients.custom_clickhouse import CustomClickhouse
import json

class ClickhouseTestCase(unittest.TestCase):
  def setUp(self):
    self.client = Client('localhost')
    self.client.execute('DROP TABLE IF EXISTS test')
    self.client.execute('CREATE TABLE test (id String, x Int32, dict String) ENGINE = ReplacingMergeTree() ORDER BY id')
    self.new_client = CustomClickhouse()

  def _add_records(self):
    documents = [{'x': 1, "id": "1"}, {'x': 2, "id": "2"}, {'x': 3, "id": "3"}, {'x': 100, "id": "100"}]
    formatted_documents = [{"_id": doc["id"], "_source": {'x': doc["x"]}} for doc in documents]
    self.client.execute(
      'INSERT INTO test (id, x) VALUES',
      documents
    )
    return formatted_documents

  def test_search(self):
    formatted_documents = self._add_records()
    result = self.new_client.search(index="test", fields=["x"])
    self.assertCountEqual(formatted_documents, result)

  def test_search_with_query(self):
    formatted_documents = self._add_records()
    formatted_documents = [doc for doc in formatted_documents if doc["_source"]['x'] < 3]
    result = self.new_client.search(index="test", query="WHERE x < 3", fields=["x"])
    self.assertSequenceEqual(formatted_documents, result)

  def test_count(self):
    formatted_documents = self._add_records()
    formatted_documents = [doc for doc in formatted_documents if doc["_source"]['x'] < 3]
    result = self.new_client.count(index="test", query="WHERE x < 3")
    assert result == len(formatted_documents)

  def test_iterate(self):
    test_per = 2
    formatted_documents = self._add_records()
    formatted_documents = [doc for doc in formatted_documents if doc["_source"]['x'] < 4]
    result = self.new_client.iterate(index="test", fields=["x"], query="WHERE x < 4", per=test_per)
    self.assertSequenceEqual(formatted_documents[0:test_per], next(result))
    self.assertSequenceEqual(formatted_documents[test_per:2*test_per], next(result))

  def test_iterate_with_other_processes(self):
    test_per = 2
    formatted_documents = self._add_records()
    result = self.new_client.iterate(index="test", fields=["x"], per=test_per)
    next(result)
    self.new_client.bulk_index(index="test", docs=[{"id": "test", "x": 0}])
    next(result)

  def test_bulk_index(self):
    documents = [{"x": i} for i in range(10)]
    self.new_client.bulk_index(index="test", docs=[d.copy() for d in documents], id_field="x")
    result = self.client.execute('SELECT id FROM test')
    self.assertCountEqual(result, [(str(doc["x"]), ) for doc in documents])

  def test_bulk_index_empty_fields(self):
    # , {"id": 2, "dict": {"x": 1}}
    documents = [{"id": 1, "x": 1}]
    self.new_client.bulk_index(index="test", docs=[d for d in documents])
    # result = self.client.execute('SELECT id FROM test')
    # self.assertCountEqual(result, [("1", ), ("2", )])

  def test_bulk_index_dict_values(self):
    documents = [{"x": i, "dict": {"test": i}} for i in range(10)]
    self.new_client.bulk_index(index="test", docs=[d.copy() for d in documents], id_field="x")
    result = self.client.execute('SELECT dict FROM test')
    self.assertCountEqual(result, [(json.dumps(doc["dict"]),) for doc in documents])

  def test_send_sql_request(self):
    formatted_documents = self._add_records()
    result = self.new_client.send_sql_request("SELECT max(x) FROM test")
    assert result == max(doc["_source"]["x"] for doc in formatted_documents)