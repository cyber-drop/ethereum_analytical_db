# search(index, query) => [{"_source": *, "_id": *}]
# count
# iterate
# send_sql_request
#
class CustomClient:
  def search(self, index, query, fields):
    pass

  def count(self, index, query):
    pass

  def iterate(self, index, query, fields):
    pass

  def send_sql_request(self, sql):
    pass

  def bulk_index(self, index, docs, id_field):
    pass