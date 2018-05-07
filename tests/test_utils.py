from pyelasticsearch import ElasticSearch
from mappings import Mappings
# Create test index
# Search ids
# Create many documents

class TestElasticSearch(ElasticSearch):
  def __init__(self):
    super().__init__("http://localhost:9200", timeout=1000)

  def recreate_index(self, index):
    try:
      self.delete_index(index)
    except:
      pass
    self.create_index(index)

  def search_ids(self, index, doc_type, query, size):
    self.search("_exists_:trace", index=TEST_INDEX, doc_type='tx', size=TEST_TRANSACTIONS_NUMBER)['hits']['hits']

  def reduce_index_size(self, index):
    mappings = Mappings(index)
    mappings.reduce_index_size()
