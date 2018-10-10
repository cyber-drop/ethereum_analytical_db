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

def get_max_block(query="*", min_consistent_block=0):
  """
  Get last block in ElasticSearch
  TODO should return max consistent block, i.e. block with max number N, for which N-1 blocks are presented in ElasticSearch

  Returns
  -------
  int:
      Last block number
      0 if there are no blocks in ElasticSearch
  """
  aggregation = {
    "size": 0,
    "query": {
      "bool": {
        "must": [
          {"query_string": {"query": query}},
          {"range": {"number": {"gte": min_consistent_block}}}
        ]
      }
    },
    "aggs": {
      "max_block": {
        "max": {
          "field": "number"
        }
      }
    }
  }
  result = client.send_request("GET", [INDICES["block"], "b", "_search"], aggregation, {})
  if result["aggregations"]["max_block"]["value"]:
    return int(result["aggregations"]["max_block"]["value"])
  else:
    return min_consistent_block

class ContractTransactionsIterator():
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

    filters = [{
      "bool": {
        "must": [
          {"terms": {"to": contracts}},
          {"range": {"blockNumber": {"gt": max_synced_block, "lte": max_block}}}
        ]
      }
    } for max_synced_block, contracts in max_blocks_contracts.items()]
    return {"bool": {"should": filters}}

  def _iterate_transactions(self, contracts, max_block, partial_query):
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
    query = {
      "bool": {
        "must": [
          partial_query,
          self._create_transactions_request(contracts, max_block)
        ]
      }
    }
    return self.client.iterate(self.indices[self.index], self.doc_type, query)

  def _get_flag_name(self):
    """
    Get name of field in which max block number should be stored

    Returns
    -------
    str
        Name of field
    """
    return "{}_{}_block".format(self.doc_type, self.block_prefix)

class ElasticSearchContractTransactionsIterator(ContractTransactionsIterator):
  def _iterate_contracts(self, max_block=None, partial_query=None):
    """
    Iterate through contracts with unprocessed transactions before specified block

    Parameters
    ----------
    max_block : int
        Block number
    partial_query : dict
        Additional ElasticSearch query

    Returns
    -------
    generator
        Generator that returns contracts with unprocessed transactions
    """
    query = {
      "bool": {
        "must": [
          partial_query,
        ]
      }
    }
    if max_block is not None:
      query["bool"]["must"].append({"bool": {
        "should": [
          {"range": {
            self._get_flag_name(): {
              "lt": max_block
            }
          }},
          {"bool": {"must_not": [{"exists": {"field": self._get_flag_name()}}]}},
        ]
      }})
    if PROCESSED_CONTRACTS:
      query["bool"]["must"].append({"terms": {"address": PROCESSED_CONTRACTS}})
    return self.client.iterate(self.indices["contract"], 'contract', query)

  def _save_max_block(self, contracts, max_block):
    """
    Save specified block value for specified contracts

    Parameters
    ----------
    contracts : list
        Contract addresses
    max_block : int
        Block number
    """
    query = {
      "terms": {
        "address": contracts
      }
    }
    self.client.update_by_query(
      index=self.indices["contract"],
      doc_type='contract',
      query=query,
      script='ctx._source.' + self._get_flag_name() + ' = ' + str(max_block)
    )

class ClickhouseContractTransactionsIterator(ContractTransactionsIterator):
  def _iterate_contracts(self, max_block=None, partial_query=None):
    query = "ANY LEFT JOIN (SELECT id, value FROM {} WHERE name = '{}') USING id WHERE value <= {} AND ".format(
      self.indices["contract_block"],
      self._get_flag_name(),
      1,
      max_block,
      partial_query
    )
    query += partial_query
    return self.client.iterate(index=self.indices["contract"], query=query, fields=[])

  def _save_max_block(self, contracts, max_block):
    docs = [{"id": contract, "name": self._get_flag_name(), "value": max_block} for contract in contracts]
    self.client.bulk_index(self.indices["contract_block"], docs)
