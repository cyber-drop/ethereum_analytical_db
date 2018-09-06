from custom_elastic_search import CustomElasticSearch
from config import INDICES, MIN_CONSISTENT_BLOCK

client = CustomElasticSearch("http://localhost:9200")

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

def get_max_block(query="*", min_consistent_block=MIN_CONSISTENT_BLOCK):
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
  def _iterate_contracts(self, max_block, partial_query):
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
          {"bool": {
            "should": [
              {"range": {
                self._get_flag_name(): {
                  "lt": max_block
                }
              }},
              {"bool": {"must_not": [{"exists": {"field": self._get_flag_name()}}]}},
            ]
          }}
        ]
      }
    }
    return self.client.iterate(self.indices["contract"], 'contract', query)

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

  def _get_flag_name(self):
    """
    Get name of field in which max block number should be stored

    Returns
    -------
    str
        Name of field
    """
    return "{}_{}_block".format(self.doc_type, self.block_prefix)