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

def get_max_block(query="*", max_block=None, min_consistent_block=MIN_CONSISTENT_BLOCK):
  client = get_elasticsearch_connection()
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
      },
      "blocks_count": {
        "value_count": {
          "field": "number"
        }
      }
    }
  }
  if max_block:
    aggregation["query"]["bool"]["must"][1]["range"]["number"]["lt"] = max_block
  result = client.send_request("GET", [INDICES["block"], "b", "_search"], aggregation, {})
  if not result['aggregations']['blocks_count']["value"]:
    return min_consistent_block
  blocks_count = int(result['aggregations']['blocks_count']["value"])
  max_block = int(result['aggregations']['max_block']["value"])
  if (max_block - min_consistent_block + 1 == blocks_count):
    return max_block
  else:
    return get_max_block(query, max_block, min_consistent_block)
