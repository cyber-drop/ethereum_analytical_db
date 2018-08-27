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
  return int(result["aggregations"]["max_block"]["value"])
