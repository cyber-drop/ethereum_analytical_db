import requests
import os
from subprocess import call
import pdb
from time import sleep
from custom_elastic_search import CustomElasticSearch
import json

SERVER_URL = "http://localhost:3000/{}"
ADD_ABI_URL = SERVER_URL.format("add_abi")
GRAB_ABI_PATH = "./quickBlocks/bin/grabABI {}"
GRAB_ABI_CACHE_PATH = "/home/anatoli/.quickBlocks/cache/abis/{}.json"

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

  def _add_contracts_abi(self, abis):
    response = requests.post(
      ADD_ABI_URL,
      headers={"content-type": "application/json"},
      data=json.dumps(abis)
    )
    return response.json()

  def _get_contract_abi(self, address):
    file_path = GRAB_ABI_CACHE_PATH.format(address)
    if not os.path.exists(file_path):
      os.system(GRAB_ABI_PATH.format(address)) 
      for attemp in range(5):
        if not os.path.exists(file_path):
          sleep(1)
    if os.path.exists(file_path):
      abi_file = open(file_path)
      return json.load(abi_file)
    else:
      return {"error": True}

  def _decode_inputs_batch(self, encoded_params):
    encoded_params_string = ",".join(encoded_params)
    response = requests.get(DECODE_PARAMS_URL.format(encoded_params_string))
    return response.json()

  def _iterate_contracts_without_abi(self):
    return self.client.iterate(self.index, 'contract', 'address:* AND !(_exists_:abi)', paginate=True)

  def _save_contracts_abi(self):
    for contracts in self._iterate_contracts_without_abi():
      abis = [self._get_contract_abi(contract["_source"]["address"]) for contract in contracts]
      operations = [self.client.update_op(doc={'abi': abis[index]}, id=contract["_id"]) for index, contract in enumerate(contracts)]
      self.client.bulk(operations, doc_type='contract', index=self.index, refresh=True)

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
    contracts = [contract['_source']['address'] for contract in contracts]
    for transactions in self._iterate_transactions_by_targets(contracts):
      inputs = [transaction["_source"]["input"] for transaction in transactions]
      decoded_inputs = self._decode_inputs_batch(inputs)
      print(decoded_inputs)
      operations = [self.client.update_op(doc={'decoded_input': decoded_inputs[index]}, id=transaction["_id"]) for index, transaction in enumerate(transactions)]
      self.client.bulk(operations, doc_type='tx', index=self.index, refresh=True)

  def decode_inputs(self):
    self._save_contracts_abi()
    for contracts in self._iterate_contracts_with_abi():
      self._add_contracts_abi([contract["_source"]["abi"] for contract in contracts]) 
      self._decode_inputs_for_contracts(contracts)   

