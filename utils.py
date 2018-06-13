from custom_elastic_search import CustomElasticSearch
from config import INDICES

client = CustomElasticSearch("http://localhost:9200")

def get_elasticsearch_connection():
  return client

def get_max_block():
  client = get_elasticsearch_connection()
  aggregation = {
    "size": 0,
    "aggs": {
      "max_block": {
        "max": {
          "field": "number"
        }
      }
    }
  }
  result = client.send_request("GET", [INDICES["block"], "b", "_search"], aggregation, {})
  return int(result['aggregations']['max_block']["value"])
