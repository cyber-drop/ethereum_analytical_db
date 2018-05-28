import os
from custom_elastic_search import CustomElasticSearch
import json
from ethereum.abi import (
    decode_abi,
    normalize_name as normalize_abi_method_name,
    method_id as get_abi_method_id)
from ethereum.utils import encode_int, zpad, decode_hex
from multiprocessing import Pool
from config import PARITY_HOSTS

GRAB_ABI_PATH = "/usr/local/qblocks/bin/grabABI {} > /dev/null 2>&1"
GRAB_ABI_CACHE_PATH = "/home/{}/.quickBlocks/cache/abis/{}.json"
NUMBER_OF_PROCESSES = 10

def _get_contracts_abi_sync(addresses):
  abis = {}
  for key, address in addresses.items():
    file_path = GRAB_ABI_CACHE_PATH.format(os.environ["USER"], address)
    if not os.path.exists(file_path):
      os.system(GRAB_ABI_PATH.format(address))
    if os.path.exists(file_path):
      abi_file = open(file_path)
      abis[key] = json.load(abi_file)
    else:
      abis[key] = []
  return abis

class Contracts():
  _contracts_abi = []

  def __init__(self, indices, host="http://localhost:9200", parity_hosts=PARITY_HOSTS):
    self.indices = indices
    self.client = CustomElasticSearch(host)
    self.pool = Pool(processes=NUMBER_OF_PROCESSES)
    self.parity_hosts = parity_hosts

  def _set_contracts_abi(self, abis):
    self._contracts_abi = abis

  def _split_on_chunks(self, iterable, size):
    iterable = iter(iterable)
    for element in iterable:
      elements = [element]
      try:
        for i in range(size - 1):
          elements.append(next(iterable))
      except StopIteration:
        pass
      yield elements

  def _get_contracts_abi(self, all_addresses):
    chunks = self._split_on_chunks(list(enumerate(all_addresses)), NUMBER_OF_PROCESSES)
    dict_chunks = [dict(chunk) for chunk in chunks]
    abis = {key: abi for abis_dict in self.pool.map(_get_contracts_abi_sync, dict_chunks) for key, abi in abis_dict.items()}
    return list(abis.values())

  # Solution from https://ethereum.stackexchange.com/questions/20897/how-to-decode-input-data-from-tx-using-python3?rq=1
  def _decode_input(self, contract, call_data):
    call_data_bin = decode_hex(call_data)
    method_signature = call_data_bin[:4]
    for description in self._contracts_abi[contract]:
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
    return [self._decode_input(contract, call_data) for contract, call_data in encoded_params]


  def _get_range_query(self):
    ranges = [range_tuple[0:2] for range_tuple in self.parity_hosts]
    range_query = self.client.make_range_query("blockNumber", *ranges)
    return range_query

  def _iterate_contracts_without_abi(self):
    return self.client.iterate(self.indices["contract"], 'contract', 'address:* AND !(_exists_:abi) AND ' + self._get_range_query())

  def _save_contracts_abi(self):
    for contracts in self._iterate_contracts_without_abi():
      abis = self._get_contracts_abi([contract["_source"]["address"] for contract in contracts])
      operations = [self.client.update_op(doc={'abi': abis[index]}, id=contract["_id"]) for index, contract in enumerate(contracts)]
      self.client.bulk(operations, doc_type='contract', index=self.indices["contract"], refresh=True)

  def _iterate_contracts_with_abi(self):
    return self.client.iterate(self.indices["contract"], 'contract', 'address:* AND _exists_:abi AND ' + self._get_range_query())

  def _iterate_transactions_by_targets(self, targets):
    query = {
      "terms": {
        "to": targets
      }
    }
    return self.client.iterate(self.indices[self.index], self.doc_type, query)

  def _decode_inputs_for_contracts(self, contracts):
    contracts = [contract['_source']['address'] for contract in contracts]
    for transactions in self._iterate_transactions_by_targets(contracts):
      inputs = [(transaction["_source"]["to"], transaction["_source"]["input"]) for transaction in transactions]
      decoded_inputs = self._decode_inputs_batch(inputs)
      operations = [self.client.update_op(doc={'decoded_input': decoded_inputs[index]}, id=transaction["_id"]) for index, transaction in enumerate(transactions)]
      self.client.bulk(operations, doc_type=self.doc_type, index=self.indices[self.index], refresh=True)

  def decode_inputs(self):
    self._save_contracts_abi()
    for contracts in self._iterate_contracts_with_abi():
      self._set_contracts_abi({contract["_source"]["address"]: contract["_source"]["abi"] for contract in contracts})
      self._decode_inputs_for_contracts(contracts)

class ExternalContracts(Contracts):
  doc_type = "tx"
  index = "transaction"

class InternalContracts(Contracts):
  doc_type = "itx"
  index = "internal_transaction"