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
import multiprocessing.pool
import functools
import utils

GRAB_ABI_PATH = "/usr/local/qblocks/bin/grabABI {} > /dev/null 2>&1"
GRAB_ABI_CACHE_PATH = "/home/{}/.quickBlocks/cache/abis/{}.json"
NUMBER_OF_PROCESSES = 10

timeout_pool = multiprocessing.pool.ThreadPool(processes=1)
# Solution from https://stackoverflow.com/questions/492519/timeout-on-a-function-call
def timeout(max_timeout):
  def timeout_decorator(item):
    @functools.wraps(item)
    def func_wrapper(*args, **kwargs):
      async_result = timeout_pool.apply_async(item, args, kwargs)
      return async_result.get(max_timeout)
    return func_wrapper
  return timeout_decorator

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

# Solution from https://ethereum.stackexchange.com/questions/20897/how-to-decode-input-data-from-tx-using-python3?rq=1
def _decode_input(contract_abi, call_data):
  call_data_bin = decode_hex(call_data)
  method_signature = call_data_bin[:4]
  for description in contract_abi:
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

def _decode_inputs_batch_sync(encoded_params):
  return {
    hash: _decode_input(contract_abi, call_data)
    for hash, (contract_abi, call_data) in encoded_params.items()
  }

class Contracts():
  _contracts_abi = {}

  def __init__(self, indices, host="http://localhost:9200", parity_hosts=PARITY_HOSTS):
    self.indices = indices
    self.client = CustomElasticSearch(host)
    self.pool = Pool(processes=NUMBER_OF_PROCESSES)
    self.parity_hosts = parity_hosts

  def _create_transactions_request(self, contracts_max_blocks, max_block):
    max_blocks_contracts = {}
    for contract, block in contracts_max_blocks.items():
      if block not in max_blocks_contracts.keys():
        max_blocks_contracts[block] = []
      max_blocks_contracts[block].append(contract)

    filters = [{
      "bool": {
        "must": [
          {"terms": {"to": contracts}},
          {"range": {"blockNumber": {"gt": max_synced_block, "lte": max_block}}}
        ]
      }
    } for max_synced_block, contracts in max_blocks_contracts.items()]
    return {"bool": {"should": filters}}

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

  def _decode_inputs_batch(self, encoded_params):
    chunks = list(self._split_on_chunks(list(encoded_params.items()), NUMBER_OF_PROCESSES))
    chunks = [dict(chunk) for chunk in chunks]
    decoded_inputs = self.pool.map(_decode_inputs_batch_sync, chunks)
    return {hash: input for chunk in decoded_inputs for hash, input in chunk.items()}

  def _get_range_query(self):
    ranges = [range_tuple[0:2] for range_tuple in self.parity_hosts]
    range_query = self.client.make_range_query("blockNumber", *ranges)
    return range_query

  def _iterate_contracts_without_abi(self):
    return self.client.iterate(self.indices["contract"], 'contract', 'address:* AND !(_exists_:abi_extracted) AND ' + self._get_range_query())

  def save_contracts_abi(self):
    for contracts in self._iterate_contracts_without_abi():
      abis = self._get_contracts_abi([contract["_source"]["address"] for contract in contracts])
      operations = [self.client.update_op(doc={'abi': abis[index], 'abi_extracted': True}, id=contract["_id"]) for index, contract in enumerate(contracts)]
      self.client.bulk(operations, doc_type='contract', index=self.indices["contract"], refresh=True)

  def _iterate_contracts_with_abi(self, max_block):
    query = {
      "bool": {
        "must": [
          {"exists": {"field": "address"}},
          {"exists": {"field": "abi"}},
          {"query_string": {"query": self._get_range_query()}},
          {"bool": {
            "should": [
              {"range": {
                self.doc_type + "_inputs_decoded_block": {
                  "lt": max_block
                }
              }},
              {"bool": {"must_not": [{"exists": {"field": self.doc_type + "_inputs_decoded_block"}}]}},
            ]
          }}
        ]
      }
    }
    return self.client.iterate(self.indices["contract"], 'contract', query)

  def _save_inputs_decoded(self, contracts, max_block):
    query = {
      "terms": {
        "address": contracts
      }
    }
    self.client.update_by_query(
      index=self.indices["contract"],
      doc_type='contract',
      query=query,
      script='ctx._source.' + self.doc_type + '_inputs_decoded_block = ' + str(max_block)
    )

  def _decode_inputs_for_contracts(self, contracts, max_block):
    contracts = {
      contract['_source']['address']: contract['_source'].get(self.doc_type + '_inputs_decoded_block', 0)
      for contract in contracts
    }
    for transactions in self._iterate_transactions_by_targets(contracts, max_block):
      try:
        inputs = {
          transaction["_id"]: (
            self._contracts_abi[transaction["_source"]["to"]],
            transaction["_source"]["input"]
          )
          for transaction in transactions
        }
        decoded_inputs = self._decode_inputs_batch(inputs)
        operations = [self.client.update_op(doc={'decoded_input': input}, id=hash) for hash, input in decoded_inputs.items()]
        self.client.bulk(operations, doc_type=self.doc_type, index=self.indices[self.index], refresh=True)
      except:
        pass

  def decode_inputs(self):
    max_block = utils.get_max_block()
    for contracts in self._iterate_contracts_with_abi(max_block):
      self._set_contracts_abi({contract["_source"]["address"]: contract["_source"]["abi"] for contract in contracts})
      self._decode_inputs_for_contracts(contracts, max_block)
      self._save_inputs_decoded([contract["_source"]["address"] for contract in contracts], max_block)

class ExternalContracts(Contracts):
  doc_type = "tx"
  index = "transaction"

  def _iterate_transactions_by_targets(self, targets, max_block):
    query = self._create_transactions_request(targets, max_block)
    return self.client.iterate(self.indices[self.index], self.doc_type, query)

class InternalContracts(Contracts):
  doc_type = "itx"
  index = "internal_transaction"

  def _iterate_transactions_by_targets(self, targets, max_block):
    query = {
      "bool": {
        "must": [
          {
            "term": {
              "callType": "call"
            }
          }
        ]
      }
    }
    query["bool"]["must"].append(self._create_transactions_request(targets, max_block))
    return self.client.iterate(self.indices[self.index], self.doc_type, query)
