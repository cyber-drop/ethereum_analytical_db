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
    contract_fields = 'id String, address String, blockNumber Int64, test UInt8, standards Array(Nullable(String)), standard_erc20 UInt8'
    if "contract" in indices:
      self.send_sql_request("CREATE TABLE IF NOT EXISTS {} ({}) {}".format(indices["contract"], contract_fields, engine))

  def index(self, index, doc, id):
    doc['id'] = id
    self.bulk_index(index=index, docs=[doc])
