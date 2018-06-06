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

def _get_state(token, address_field):
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
  result = client.send_request("GET", [app.config["token_tx"], "tx", "_search"], aggregation, {})
  documents = result['aggregations']['holders']["buckets"]
  return {document["key"]: float(document["state"]["value"]) for document in documents}

def get_incomes(token):
  return _get_state(token, "to.keyword")

def get_outcomes(token):
  return _get_state(token, "from.keyword")

def get_balances(token):
  incomes = get_incomes(token)
  outcomes = get_outcomes(token)
  addresses = list(incomes.keys()) + list(outcomes.keys())
  return {address: incomes.get(address, 0) - outcomes.get(address, 0) for address in addresses}

@app.route("/balances")
def get_balances_api():
  token = request.args.get("token")
  return jsonify(get_balances(token))