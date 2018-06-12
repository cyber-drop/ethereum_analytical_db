import flask
from flask import Flask, jsonify, request
from custom_elastic_search import CustomElasticSearch
from config import INDICES

app = Flask(__name__)
app.config.update(INDICES)

BLOCKS_CHUNK_SIZE = 10000

def get_holders_number(token):
  client = CustomElasticSearch("http://localhost:9200")
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

def _get_state(token, address_field, block):
  client = CustomElasticSearch("http://localhost:9200")
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
  return _get_state(token, "to.keyword", block)

def get_token_outcomes(token, block=None):
  return _get_state(token, "from.keyword", block)

def _get_internal_ethereum_state(field, start, end, index="internal_transaction", value="value"):
  client = CustomElasticSearch("http://localhost:9200")
  aggregation = {
    "size": 10,
    "query": {
      "bool": {
        "must_not": [
          {"term": {value: hex(0)}}
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
              "script": """
                double value = 0.0;
                int size = 7;
                String stringValue = doc['{value}'].value.substring(2);
                String chunk = "";
                for (int i = 0; i < stringValue.length(); i++) {{
                  chunk += stringValue.charAt(i);
                  if (i%size == 0) {{
                    long longChunk = Long.parseLong(chunk, 16);
                    value = value*(1 << (size * 4)) + longChunk;
                    chunk = "";
                  }}
                }}
                if (chunk.length() > 0) {{
                  long longChunk = Long.parseLong(chunk, 16);
                  value = value*(1 << (chunk.length() * 4)) + longChunk;
                }}
                return value / 1e18
              """.format(value=value)
            }
          }
        }
      }
    }
  }
  if start and end:
    aggregation["query"]["bool"]["must"] = [{
      "range": {
        "blockNumber": {
          "lte": end,
          "gte": start
        }
      }
    }]
  result = client.send_request("GET", [app.config[index], "_search"], aggregation, {})
  print(result)
  documents = result['aggregations']['holders']["buckets"]
  return {document["key"]: float(document["state"]["value"]) for document in documents}

def get_internal_ethereum_incomes(start=None, end=None):
  return _get_internal_ethereum_state("to", start, end)

def get_internal_ethereum_outcomes(start=None, end=None):
  return _get_internal_ethereum_state("from", start, end)

def get_ethereum_rewards(start=None, end=None):
  return _get_internal_ethereum_state("author.keyword", start, end, index="miner_transaction", value="value.keyword")

def _get_ethereum_state(field, start, end):
  client = CustomElasticSearch("http://localhost:9200")
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
  result = client.send_request("GET", [app.config["transaction"], "_search"], aggregation, {})
  documents = result['aggregations']['holders']["buckets"]
  return {document["key"]: float(document["state"]["value"]) for document in documents}

def get_ethereum_incomes(start=None, end=None):
  return _get_ethereum_state("to", start, end)

def get_ethereum_outcomes(start=None, end=None):
  return _get_ethereum_state("from", start, end)

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
  for start, end in _split_range(0, block, BLOCKS_CHUNK_SIZE):
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
    block = None
  return jsonify(get_ethereum_balances(block))