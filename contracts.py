import requests
import os
from subprocess import call
import pdb
from time import sleep
from custom_elastic_search import CustomElasticSearch
import json

SERVER_URL = "http://localhost:3000/{}"
ADD_ABI_URL = SERVER_URL.format("add_abi")
GET_ABI_URL = SERVER_URL.format("get_abi/{}")
DECODE_PARAMS_URL = SERVER_URL.format("decode_params/{}")

class Contracts():
  def __init__(self, index, host="http://localhost:9200"):
    self.index = index
    self.client = CustomElasticSearch()
    self._restart_server()

  def _restart_server(self):
    sleep(1)
    os.system("kill `lsof -i tcp:3000 | awk 'NR == 2 {print $2}'`")
    call('node ethereum_contracts_server/server.js &', shell=True)
    sleep(3)

  def _add_contract_abi(self, abi):
    response = requests.post(
      ADD_ABI_URL,
      data=json.dumps(abi)
    )
    return response.json()

  def _get_contract_abi(self, address):
    response = requests.get(GET_ABI_URL.format(address))
    return response.json()

  def _decode_inputs_batch(self, encoded_params):
    encoded_params_string = ",".join(encoded_params)
    response = requests.get(DECODE_PARAMS_URL.format(encoded_params_string))
    return response.json()

  def _iterate_contracts_without_abi(self):
    return self.client.iterate(self.index, 'contract', 'address:* AND !(_exists_:abi)', paginate=True)

  def _iterate_contracts_with_abi(self):
    return self.client.iterate(self.index, 'contract', 'address:* AND _exists_:abi', paginate=True)

  def _iterate_transactions_by_targets(self, targets):
    query = {
      "terms": {
        "to": targets
      }
    }
    return self.client.iterate(self.index, 'tx', query, paginate=True)

  def _decode_inputs_for_contracts(self, contracts):
    for contract in contracts:
      # Remove unsuccessfuly received contracts
      self._add_contract_abi(contract)
    for transactions in self._iterate_transactions_by_targets(contracts):
      inputs = [transaction["_source"]["input"] for transaction in transactions]
      decoded_inputs = self._decode_inputs_batch(inputs)
      operations = [self.client.update_op(doc={'decoded_input': decoded_inputs[index]}, id=transaction["_id"]) for index, transaction in enumerate(transactions)]
      self.client.bulk(operations, doc_type='tx', index=self.index, refresh=True)

  def decode_inputs(self):
    for contracts in self._iterate_contracts():
      contracts = [contract['_source']['address'] for contract in contracts]
      self._decode_inputs_for_contracts(contracts)      

