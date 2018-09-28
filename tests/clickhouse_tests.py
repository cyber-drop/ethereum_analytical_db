import unittest
from clickhouse_driver import Client
from clients.custom_clickhouse import CustomClickhouse

class ClickhouseTestCase(unittest.TestCase):
  def setUp(self):
    self.client = Client('localhost')
    self.client.execute('DROP TABLE IF EXISTS test')
    self.client.execute('CREATE TABLE test (x Int32) ENGINE = Memory')
    self.new_client = CustomClickhouse()

  def _add_records(self):
    documents = [{'x': 1}, {'x': 2}, {'x': 3}, {'x': 100}]
    formatted_documents = [{"_source": doc} for doc in documents]
    self.client.execute(
      'INSERT INTO test (x) VALUES',
      documents
    )
    return formatted_documents

  def test_search(self):
    formatted_documents = self._add_records()
    result = self.new_client.search(index="test", fields=["x"])
    self.assertSequenceEqual(formatted_documents, result)

  def test_search_with_query(self):
    formatted_documents = self._add_records()
    formatted_documents = [doc for doc in formatted_documents if doc["_source"]['x'] < 3]
    result = self.new_client.search(index="test", query="WHERE x < 3", fields=["x"])
    self.assertSequenceEqual(formatted_documents, result)

  def test_iterate(self):
    test_per = 2
    formatted_documents = self._add_records()
    formatted_documents = [doc for doc in formatted_documents if doc["_source"]['x'] < 4]
    result = self.new_client.iterate(index="test", fields=["x"], query="WHERE x < 4", per=test_per)
    self.assertSequenceEqual(formatted_documents[0:test_per], next(result))
    self.assertSequenceEqual(formatted_documents[test_per:2*test_per], next(result))

  def test_bulk_index(self):
    documents = [{"x": i} for i in range(10)]
    self.new_client.bulk_index(index="test", docs=documents)
    result = self.client.execute('SELECT x FROM test')
    self.assertCountEqual(result, [(doc["x"], ) for doc in documents])

  def test_send_sql_request(self):
    formatted_documents = self._add_records()
    result = self.new_client.send_sql_request("SELECT max(x) FROM test")
    assert result == max(doc["_source"]["x"] for doc in formatted_documents)
