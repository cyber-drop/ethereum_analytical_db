from custom_elastic_search import CustomElasticSearch
from config import INDICES, MIN_CONSISTENT_BLOCK

client = CustomElasticSearch("http://localhost:9200")

def get_elasticsearch_connection():
  return client

def split_on_chunks(iterable, size):
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

  def _create_transactions_request(self, contracts_max_blocks, max_block):
    max_blocks_contracts = {}
    for contract, block in contracts_max_blocks.items():
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
    targets = {
      contract['_source']['address']: contract['_source'].get(self._get_flag_name(), 0)
      for contract in contracts
    }
    query = {
      "bool": {
        "must": [
          partial_query,
          self._create_transactions_request(targets, max_block)
        ]
      }
    }
    return self.client.iterate(self.indices[self.index], self.doc_type, query)

  def _save_max_block(self, contracts, max_block):
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
    return "{}_{}_block".format(self.doc_type, self.block_prefix)