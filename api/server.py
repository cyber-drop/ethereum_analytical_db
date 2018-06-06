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
          {"range": {"value": {"lte": 1e10}}},
          {"term": {"token": token}},
          {"exists": {"field": "value"}}
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

def get_incomes(token, block=None):
  return _get_state(token, "to.keyword", block)

def get_outcomes(token, block=None):
  return _get_state(token, "from.keyword", block)

def get_balances(token, block=None):
  incomes = get_incomes(token, block)
  outcomes = get_outcomes(token, block)
  addresses = list(incomes.keys()) + list(outcomes.keys())
  return {address: incomes.get(address, 0) - outcomes.get(address, 0) for address in addresses}

@app.route("/balances")
def get_balances_api():
  token = request.args.get("token")
  block = int(request.args.get("block", 0))
  if not block:
    block = None
  return jsonify(get_balances(token, block))