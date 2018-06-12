import flask
from flask import Flask, jsonify, request
from custom_elastic_search import CustomElasticSearch
from config import INDICES

app = Flask(__name__)
app.config.update(INDICES)

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

def _get_internal_ethereum_state(field, block):
  client = CustomElasticSearch("http://localhost:9200")
  aggregation = {
    "size": 0,
    "query": {
      "bool": {
        "must_not": [
          {"term": {"value": hex(0)}}
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
                String stringValue = doc['value'].value.substring(2);
                if (stringValue.length() == 0)
                  return 0.0;
                String chunk = "";
                for (int i = 0; i < stringValue.length(); i++) {
                  chunk += stringValue.charAt(i);
                  if (i%size == 0) {
                    long longChunk = Long.parseLong(chunk, 16);
                    value = value*(1 << (size * 4)) + longChunk;
                    chunk = "";
                  }
                } 
                if (chunk.length() > 0) {
                  long longChunk = Long.parseLong(chunk, 16);
                  value = value*(1 << (chunk.length() * 4)) + longChunk;
                }
                return value / 1e18
              """
            }
          }
        }
      }
    }
  }
  if block:
    aggregation["query"]["bool"]["must"] = [{
      "range": {
        "blockNumber": {
          "lte": block
        }
      }
    }]
  result = client.send_request("GET", [app.config["internal_transaction"], "_search"], aggregation, {})
  documents = result['aggregations']['holders']["buckets"]
  return {document["key"]: float(document["state"]["value"]) for document in documents}

def get_internal_ethereum_incomes(block=None):
  return _get_internal_ethereum_state("to", block)

def get_internal_ethereum_outcomes(block=None):
  return _get_internal_ethereum_state("from", block)

def _get_ethereum_state(field, block):
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
  if block:
    aggregation["query"]["bool"]["must"].append({
      "range": {
        "blockNumber": {
          "lte": block
        }
      }
    })
  indices = ",".join([app.config["transaction"], app.config["internal_transaction"]])
  result = client.send_request("GET", [indices, "_search"], aggregation, {})
  documents = result['aggregations']['holders']["buckets"]
  return {document["key"]: float(document["state"]["value"]) for document in documents}

def get_ethereum_incomes(block=None):
  return _get_ethereum_state("to", block)

def get_ethereum_outcomes(block=None):
  return _get_ethereum_state("from", block)

def get_token_balances(token, block=None):
  incomes = get_token_incomes(token, block)
  outcomes = get_token_outcomes(token, block)
  addresses = set(list(incomes.keys()) + list(outcomes.keys()))
  return {address: incomes.get(address, 0) - outcomes.get(address, 0) for address in addresses}

def get_ethereum_balances(block=None):
  incomes = get_ethereum_incomes(block)
  outcomes = get_ethereum_outcomes(block)
  internal_incomes = get_internal_ethereum_incomes(block)
  internal_outcomes = get_internal_ethereum_outcomes(block)
  addresses = set(
    list(incomes.keys()) +
    list(outcomes.keys()) +
    list(internal_incomes.keys()) +
    list(internal_outcomes.keys())
  )
  return {
    address: incomes.get(address, 0)
             - outcomes.get(address, 0)
             + internal_incomes.get(address, 0)
             - internal_outcomes.get(address, 0)
    for address in addresses
  }

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