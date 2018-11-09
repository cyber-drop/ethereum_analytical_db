from pyelasticsearch import ElasticSearch
from clients.custom_elastic_search import CustomElasticSearch
from clients.custom_clickhouse import CustomClickhouse
from unittest.mock import MagicMock
from operations.indices import ClickhouseIndices

def mockify(object, mocks, not_mocks):
  def cat(x=None, *args, **kwargs):
    return x
  for attr in  dir(object):
    if not attr.startswith('__'):
      if attr in mocks.keys():
        setattr(object, attr, mocks[attr])
      elif attr not in not_mocks:
        value = getattr(object, attr)
        if callable(value):
          setattr(object, attr, MagicMock(side_effect=cat))

class TestClickhouse(CustomClickhouse):
  def prepare_indices(self, indices):
    for index in indices.values():
      self.send_sql_request("DROP TABLE IF EXISTS {}".format(index))
    ClickhouseIndices(indices).prepare_indices()
    self._prepare_views_as_indices(indices)

  def _prepare_views_as_indices(self, indices):
    engine = 'ENGINE = ReplacingMergeTree() ORDER BY id'
    contract_fields = 'id String, address String, blockNumber Int64, test UInt8'
    if "contract" in indices:
      self.send_sql_request("CREATE TABLE IF NOT EXISTS {} ({}) {}".format(indices["contract"], contract_fields, engine))

  def index(self, index, doc, id):
    doc['id'] = id
    self.bulk_index(index=index, docs=[doc])

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