import flask
from flask import Flask, jsonify, request
from custom_elastic_search import CustomElasticSearch
from config import INDICES
from tqdm import *

app = Flask(__name__)
app.config.update(INDICES)

BLOCKS_CHUNK_SIZE = 100000

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
  result = client.send_request("GET", [app.config["block"], "b", "_search"], aggregation, {})
  return int(result['aggregations']['max_block']["value"])


def get_holders_number(token):
  client = get_elasticsearch_connection()
  aggregation = {
    "size": 0,
    "query": {
      "term": {
        "token": token
      }
    },
    "aggs": {
      "senders": {
        "cardinality": {
          "field": "to.keyword"
        }
      },
      "receivers": {
        "cardinality": {
          "field": "from.keyword"
        }
      }
    }
  }
  result = client.send_request("GET", [app.config["token_tx"], "tx", "_search"], aggregation, {})
  return result['aggregations']['senders']["value"] + result["aggregations"]["receivers"]["value"]

def _get_token_state(token, address_field, block):
  client = get_elasticsearch_connection()
  aggregation = {
    "size": 0,
    "query": {
      "bool": {
        "must": [
          {"term": {"token": token}},
          {"exists": {"field": "value"}}
        ],
        "must_not": [
          {"term": {"valid": False}},
          {"term": {"method": "approve"}}
        ]
      }

    },
    "aggs": {
      "holders": {
        "terms": {
          "field": address_field,
          "size": get_holders_number(token)
        },
        "aggs": {
          "state": {
            "sum": {
              "field": "value"
            }
          }
        }
      }
    }
  }
  if block:
    aggregation["query"]["bool"]["must"].append({"range": {"block_id": {"lte": block}}})
  result = client.send_request("GET", [app.config["token_tx"], "tx", "_search"], aggregation, {})
  documents = result['aggregations']['holders']["buckets"]
  return {document["key"]: float(document["state"]["value"]) for document in documents}

def get_token_incomes(token, block=None):
  return _get_token_state(token, "to.keyword", block)

def get_token_outcomes(token, block=None):
  return _get_token_state(token, "from.keyword", block)

def _get_ethereum_state(field, start, end, index="transaction"):
  client = get_elasticsearch_connection()
  aggregation = {
    "size": 0,
    "query": {
      "bool": {
        "must": [
          {"range": {"value": {"gt": 0}}}
        ]
      }
    },
    "aggs": {
      "holders": {
        "terms": {
          "field": field
        },
        "aggs": {
          "state": {
            "sum": {
              "field": "value"
            }
          }
        }
      }
    }
  }
  if start and end:
    aggregation["query"]["bool"]["must"].append({
      "range": {
        "blockNumber": {
          "lte": start,
          "gte": end
        }
      }
    })
  result = client.send_request("GET", [app.config[index], "_search"], aggregation, {})
  documents = result['aggregations']['holders']["buckets"]
  return {document["key"]: float(document["state"]["value"]) for document in documents}

def get_ethereum_incomes(start=None, end=None):
  return _get_ethereum_state("to", start, end)

def get_ethereum_outcomes(start=None, end=None):
  return _get_ethereum_state("from", start, end)

def get_internal_ethereum_incomes(start=None, end=None):
  return _get_ethereum_state("to", start, end, index="internal_transaction")

def get_internal_ethereum_outcomes(start=None, end=None):
  return _get_ethereum_state("from", start, end, index="internal_transaction")

def get_ethereum_rewards(start=None, end=None):
  return _get_ethereum_state("author.keyword", start, end, index="miner_transaction")

def get_token_balances(token, block=None):
  incomes = get_token_incomes(token, block)
  outcomes = get_token_outcomes(token, block)
  addresses = set(list(incomes.keys()) + list(outcomes.keys()))
  return {address: incomes.get(address, 0) - outcomes.get(address, 0) for address in addresses}

def _split_range(start, end, size):
  ranges = list(zip(list(range(start, end, size)), list(range(start + size - 1, end, size))))
  ranges.append((start + int((end - start) / size) * size, end))
  return ranges

def get_ethereum_balances(block=None):
  final_balances = {}
  for start, end in tqdm(_split_range(0, block, BLOCKS_CHUNK_SIZE)):
    incomes = get_ethereum_incomes(start, end)
    outcomes = get_ethereum_outcomes(start, end)
    internal_incomes = get_internal_ethereum_incomes(start, end)
    internal_outcomes = get_internal_ethereum_outcomes(start, end)
    rewards = get_ethereum_rewards(start, end)
    addresses = set(
      list(incomes.keys()) +
      list(outcomes.keys()) +
      list(internal_incomes.keys()) +
      list(internal_outcomes.keys()) +
      list(rewards.keys())
    )
    balances = {
      address: incomes.get(address, 0)
               - outcomes.get(address, 0)
               + internal_incomes.get(address, 0)
               - internal_outcomes.get(address, 0)
               + rewards.get(address, 0)
      for address in addresses
    }
    for address in balances.keys():
      if address not in final_balances.keys():
        final_balances[address] = 0
      final_balances[address] += balances[address]

  return final_balances


@app.route("/token_balances")
def get_token_balances_api():
  token = request.args.get("token")
  block = int(request.args.get("block", 0))
  if not block:
    block = None
  return jsonify(get_token_balances(token, block))

@app.route("/ethereum_balances")
def get_ethereum_balances_api():
  block = int(request.args.get("block", 0))
  if not block:
    block = get_max_block()
  return jsonify(get_ethereum_balances(block))