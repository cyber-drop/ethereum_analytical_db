from clickhouse_driver import Client
from utils import split_on_chunks
from config import NUMBER_OF_JOBS

class CustomClickhouse:
  def __init__(self):
    self.client = Client('localhost')

  def _create_sql_query(self, index, query, fields):
    fields_string = ",".join(fields)
    sql = 'SELECT {} FROM {}'.format(fields_string, index)
    if query:
      sql += ' ' + query
    return sql

  def _convert_values_to_dict(self, values, fields):
    return [{"_source": dict(zip(fields, value))} for value in values]

  def search(self, index, fields, query=None):
    sql = self._create_sql_query(index, query, fields)
    values = self.client.execute(sql)
    return self._convert_values_to_dict(values, fields)

  def iterate(self, index, fields, query=None, per=NUMBER_OF_JOBS):
    settings = {'max_block_size': per}
    sql = self._create_sql_query(index, query, fields)
    generator = self.client.execute_iter(sql, settings=settings)
    for chunk in split_on_chunks(generator, per):
      yield self._convert_values_to_dict(chunk, fields)

  def bulk_index(self, index, docs):
    fields = list(set([field for doc in docs for field in doc.keys()]))
    fields_string = ",".join(fields)
    self.client.execute(
      'INSERT INTO {} ({}) VALUES'.format(index, fields_string),
      docs
    )

  def send_sql_request(self, sql):
    return self.client.execute(sql)[0][0]