from clients.custom_elastic_search import CustomElasticSearch
from config import INDICES, PROCESSED_CONTRACTS

client = CustomElasticSearch("http://localhost:9200")

def make_range_query(field, range_tuple, *args):
  """
  Create SQL request to get all documents with specified field in specified range

  Parameters
  ----------
  field : string
      Contracts info in ElasticSearch JSON format, i.e.
      {"_id": TRANSACTION_ID, "_source": {"document": "fields"}}
  range_tuple : int
      Tuple in a format of (start_block, end_block)
  *args : list
      Other tuples, or empty

  Returns
  -------
  str
      SQL query in a form of:
      (field >= 1 AND field <= 2) OR (field >= 4)
  """
  if len(args):
    requests = ["({})".format(make_range_query(field, range_tuple)) for range_tuple in [range_tuple] + list(args)]
    result_request = " OR ".join(requests)
    return result_request
  else:
    bottom_line = range_tuple[0]
    upper_bound = range_tuple[1]
    if (bottom_line is not None) and (upper_bound is not None):
      return "{0} >= {1} AND {0} < {2}".format(field, bottom_line, upper_bound)
    elif (bottom_line is not None):
      return "{} >= {}".format(field, bottom_line)
    elif (upper_bound is not None):
      return "{} < {}".format(field, upper_bound)
    else:
      return "{} IS NOT NULL".format(field)

def get_elasticsearch_connection():
  """
  Establish ElasticSearch connection

  TODO recreate connection each time it fails or remove this method

  Returns
  -------
  CustomElasticSearch
      ElasticSearch client
  """
  return client

def split_on_chunks(iterable, size):
  """
  Split given iterable onto chunks

  Parameters
  ----------
  iterable : generator
      Iterable that will be splitted
  size : int
      Max size of chunk
  Returns
  -------
  generator
      Generator that returns chunk on each iteration
  """
  iterable = iter(iterable)
  for element in iterable:
    elements = [element]
    try:
      for i in range(size - 1):
        elements.append(next(iterable))
    except StopIteration:
      pass
    yield elements

class ClickhouseContractTransactionsIterator():
  def _iterate_contracts(self, max_block=None, partial_query=None, fields=[]):
    query = partial_query
    if max_block is not None:
      inner_query = "SELECT id FROM {} WHERE name = '{}' AND value >= {}".format(
        self.indices["contract_block"],
        self._get_flag_name(),
        max_block
      )
      query += " AND id NOT in({})".format(inner_query)
    if PROCESSED_CONTRACTS:
      addresses = ",".join(["'{}'".format(address) for address in PROCESSED_CONTRACTS])
      query += " AND address in({})".format(addresses)
    return self.client.iterate(index=self.indices["contract"], query=query, fields=fields)

  def _create_transactions_request(self, contracts, max_block):
    """
    Create ElasticSearch request to get transactions for all contracts
    from last processed block to specified block

    Parameters
    ----------
    contracts : list
        Contracts info in ElasticSearch JSON format, i.e.
        {"_id": TRANSACTION_ID, "_source": {"document": "fields"}}
    max_block : int
        Block number

    Returns
    -------
    dict
        ElasticSearch request to get transactions by conditions above
    """
    max_blocks_contracts = {}
    for contract_dict in contracts:
      block = contract_dict["_source"].get(self._get_flag_name(), 0)
      contract = contract_dict["_source"]["address"]
      if block not in max_blocks_contracts.keys():
        max_blocks_contracts[block] = []
      max_blocks_contracts[block].append(contract)

    query = []
    for max_synced_block, contracts in max_blocks_contracts.items():
      if len(contracts) == 1:
        contracts_string = "= '{}'".format(contracts[0])
      else:
        contracts_string = "in({})".format(", ".join(["'{}'".format(contract) for contract in contracts]))
      if max_synced_block > 0:
        subquery = "(to {} AND blockNumber > {} AND blockNumber <= {})".format(
          contracts_string,
          max_synced_block,
          max_block
        )
      else:
        subquery = "(to {})".format(contracts_string)
      query.append(subquery)
    return " OR ".join(query)

  def _iterate_transactions(self, contracts, max_block, partial_query, fields=[]):
    """
    Iterate through unprocessed transactions for specified contracts before specified block

    Parameters
    ----------
    contracts : list
        Contracts info in ElasticSearch JSON format, i.e.
        {"_id": TRANSACTION_ID, "_source": {"document": "fields"}}
    max_block : int
        Block number
    partial_query : dict
        Additional ElasticSearch query

    Returns
    -------
    generator
        Generator that returns unprocessed transactions
    """
    query = partial_query
    print(self._create_transactions_request(contracts, max_block))
    query += " AND " + self._create_transactions_request(contracts, max_block)
    return self.client.iterate(index=self.indices[self.index], fields=fields, query=query)

  def _get_flag_name(self):
    """
    Get name of field in which max block number should be stored

    Returns
    -------
    str
        Name of field
    """
    return "{}_{}_block".format(self.doc_type, self.block_prefix)

  def _save_max_block(self, contracts, max_block):
    docs = [{"id": contract, "name": self._get_flag_name(), "value": max_block} for contract in contracts]
    self.client.bulk_index(self.indices["contract_block"], docs)

  def _get_max_block(self, query={}, min_consistent_block=0):
    query_string = " OR ".join(["(name = '{}' AND value = {})".format(field, value) for field, value in query.items()])
    sql = "SELECT MAX(toInt32(id))"
    if query_string:
      sql += " FROM {} WHERE {}".format(self.indices["block_flag"], query_string)
    else:
      sql += " FROM {}".format(self.indices["block"])
    return max(self.client.send_sql_request(sql), min_consistent_block)
