import requests
import os
from subprocess import call
import pdb
from time import sleep
from custom_elastic_search import CustomElasticSearch

SERVER_URL = "http://localhost:3000/{}"
ADD_ABI_URL = SERVER_URL.format("add_abi/{}")
DECODE_PARAMS_URL = SERVER_URL.format("decode_params/{}")

class Contracts():
  def __init__(self, index, host="http://localhost:9200"):
    self._restart_server()
    self.index = index
    self.client = CustomElasticSearch()

  def _restart_server(self):
    os.system("kill `lsof -i tcp:3000 | awk 'NR == 2 {print $2}'`")
    call('node ethereum_contracts_server/server.js &', shell=True)
    sleep(3)

  def _add_contract_abi(self, address):
    response = requests.get(ADD_ABI_URL.format(address))
    return response.json()

  def _decode_input(self, encoded_params):
    response = requests.get(DECODE_PARAMS_URL.format(encoded_params))
    if response.text:
      return response.json()
    else:
      return {'contract_without_abi': True}

  def _iterate_contracts(self):
    return self.client.iterate(self.index, 'contract', 'address:*', paginate=True)

  def _iterate_transactions_by_targets(self, targets):
    query = {
      "terms": {
        "to": targets
      }
    }
    return self.client.iterate(self.index, 'tx', query, paginate=True)

  def _decode_inputs_for_contracts(self, contracts):
    for contract in contracts:
      self._add_contract_abi(contract)
    for transactions in self._iterate_transactions_by_targets(contracts):
      decoded_inputs = {transaction["_id"]: {'decoded_input': self._decode_input(transaction["_source"]["input"])} for transaction in transactions}
      operations = [self.client.update_op(doc=decoded_inputs[transaction["_id"]], id=transaction["_id"]) for transaction in transactions]
      self.client.bulk(operations, doc_type='tx', index=self.index, refresh=True)

  def decode_inputs(self):
    for contracts in self._iterate_contracts():
      contracts = [contract['_source']['address'] for contract in contracts]
      self._decode_inputs_for_contracts(contracts)      

