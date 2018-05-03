import os
from subprocess import call
import pdb
from time import sleep
from custom_elastic_search import CustomElasticSearch
import json
from ethereum.abi import (
    decode_abi,
    normalize_name as normalize_abi_method_name,
    method_id as get_abi_method_id)
from ethereum.utils import encode_int, zpad, decode_hex

GRAB_ABI_PATH = "./quickBlocks/bin/grabABI {}"
GRAB_ABI_CACHE_PATH = "/home/{}/.quickBlocks/cache/abis/{}.json"

class Contracts():
  _contracts_abi = []

  def __init__(self, index, host="http://localhost:9200"):
    self.index = index
    self.client = CustomElasticSearch()

  def _set_contracts_abi(self, abis):
    self._contracts_abi = [method for abi in abis for method in abi]

  def _get_contract_abi(self, address):
    file_path = GRAB_ABI_CACHE_PATH.format(os.environ["USER"], address)
    if not os.path.exists(file_path):
      os.system(GRAB_ABI_PATH.format(address)) 
      for attemp in range(5):
        if not os.path.exists(file_path):
          sleep(1)
    if os.path.exists(file_path):
      abi_file = open(file_path)
      return json.load(abi_file)
    else:
      return []

  # Solution from https://ethereum.stackexchange.com/questions/20897/how-to-decode-input-data-from-tx-using-python3?rq=1
  def _decode_input(self, call_data):
    call_data_bin = decode_hex(call_data)
    method_signature = call_data_bin[:4]
    for description in self._contracts_abi:
      if description.get('type') != 'function':
        continue
      method_name = normalize_abi_method_name(description['name'])
      arg_types = [item['type'] for item in description['inputs']]
      method_id = get_abi_method_id(method_name, arg_types)
      if zpad(encode_int(method_id), 4) == method_signature:
        try:
          args = decode_abi(arg_types, call_data_bin[4:])          
          args = [{'type': arg_types[index], 'value': str(value)} for index, value in enumerate(args)]
        except AssertionError:
          continue
        return {'name': method_name, 'params': args}

  def _decode_inputs_batch(self, encoded_params):
    return [self._decode_input(call_data) for call_data in encoded_params]
      
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
      operations = [self.client.update_op(doc={'decoded_input': decoded_inputs[index]}, id=transaction["_id"]) for index, transaction in enumerate(transactions)]
      self.client.bulk(operations, doc_type='tx', index=self.index, refresh=True)

  def decode_inputs(self):
    self._save_contracts_abi()
    for contracts in self._iterate_contracts_with_abi():
      self._set_contracts_abi([contract["_source"]["abi"] for contract in contracts]) 
      self._decode_inputs_for_contracts(contracts)   

