import flask
from flask import Flask, jsonify, request
from config import INDICES
from tqdm import *
import utils

app = Flask(__name__)
app.config.update(INDICES)

PARTITION_SIZE = 100

def get_holders_number(token):
  client = utils.get_elasticsearch_connection()
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

def _get_ethereum_holders_number(block, index, doc_type):
  client = utils.get_elasticsearch_connection()
  aggregation = {
    "size": 0,
    "query": {
      "range": {
        "blockNumber": {
          "lte": block
        }
      }
    },
    "aggs": {
      "senders": {
        "cardinality": {
          "field": "to"
        }
      },
      "receivers": {
        "cardinality": {
          "field": "from"
        }
      }
    }
  }
  result = client.send_request("GET", [app.config[index], doc_type, "_search"], aggregation, {})
  return result['aggregations']['senders']["value"] + result["aggregations"]["receivers"]["value"]

def _get_ethereum_external_holders_number(block):
  return _get_ethereum_holders_number(block, "transaction", "tx")

def _get_ethereum_internal_holders_number(block):
  return _get_ethereum_holders_number(block, "internal_transaction", "itx")

def _get_ethereum_miners_number(block):
  client = utils.get_elasticsearch_connection()
  aggregation = {
    "size": 0,
    "query": {
      "range": {
        "blockNumber": {
          "lte": block
        }
      }
    },
    "aggs": {
      "miners": {
        "cardinality": {
          "field": "author.keyword"
        }
      }
    }
  }
  result = client.send_request("GET", [app.config["miner_transaction"], "tx", "_search"], aggregation, {})
  return result['aggregations']['miners']["value"]

def get_ethereum_holders_number(block):
  return _get_ethereum_external_holders_number(block) \
         + _get_ethereum_internal_holders_number(block) \
         + _get_ethereum_miners_number(block)

def _get_token_state(token, address_field, block):
  client = utils.get_elasticsearch_connection()
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

def _get_ethereum_state(field, block, index="transaction"):
  client = utils.get_elasticsearch_connection()
  final_result = {}
  size = get_ethereum_holders_number(block)
  partitions = int(size / PARTITION_SIZE)
  for partition in range(partitions):
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
            "field": field,
            "size": size,
            "include": {
              "partition": partition,
              "num_partitions": partitions
            }
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
      aggregation["query"]["bool"]["must"].append({
        "range": {
          "blockNumber": {
            "lte": block
          }
        }
      })
    result = client.send_request("GET", [app.config[index], "_search"], aggregation, {})
    documents = result['aggregations']['holders']["buckets"]
    partial_result = {document["key"]: float(document["state"]["value"]) for document in documents}
    final_result.update(partial_result)
  return final_result

def get_ethereum_incomes(block=None):
  return _get_ethereum_state("to", block)

def get_ethereum_outcomes(block=None):
  return _get_ethereum_state("from", block)

def get_internal_ethereum_incomes(block=None):
  return _get_ethereum_state("to", block, index="internal_transaction")

def get_internal_ethereum_outcomes(block=None):
  return _get_ethereum_state("from", block, index="internal_transaction")

def get_ethereum_rewards(block=None):
  return _get_ethereum_state("author.keyword", block, index="miner_transaction")

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
  incomes = get_ethereum_incomes(block)
  outcomes = get_ethereum_outcomes(block)
  internal_incomes = get_internal_ethereum_incomes(block)
  internal_outcomes = get_internal_ethereum_outcomes(block)
  rewards = get_ethereum_rewards(block)
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
  return balances

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
    block = utils.get_max_block()
  return jsonify(get_ethereum_balances(block))