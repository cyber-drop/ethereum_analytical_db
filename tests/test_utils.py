from pyelasticsearch import ElasticSearch
from custom_elastic_search import CustomElasticSearch

class TestElasticSearch(ElasticSearch):
  def __init__(self):
    super().__init__("http://localhost:9200", timeout=1000)
    self.client = CustomElasticSearch("http://localhost:9200")

  def recreate_index(self, index):
    try:
      self.delete_index(index)
    except:
      pass
    self.create_index(index)

  def search_ids(self, index, doc_type, query, size):
    self.search("_exists_:trace", index=TEST_INDEX, doc_type='tx', size=TEST_TRANSACTIONS_NUMBER)['hits']['hits']

  def recreate_fast_index(self, index, doc_type='tx'):
    try:
      self.delete_index(index)
    except:
      pass
    self.client.prepare_fast_index(index, doc_type)