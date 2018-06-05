import flask
from flask import Flask, jsonify, request
from custom_elastic_search import CustomElasticSearch

app = Flask(__name__)

@app.route('/holders')
def get_holders():
  token = request.args.get("token")
  client = CustomElasticSearch("http://localhost:9200")
  token_holders = []
  for transactions in client.iterate(index="test-ethereum-transactions", doc_type="tx", query="token:" + token):
    for transaction in transactions:
      token_holders += [transaction["_source"]["from"], transaction["_source"]["to"]]
  return jsonify(list(set(token_holders)))
