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

def _get_contracts_abi_sync(addresses):
  """
  Get ABIs for specified list of addresses

  Parameters
  ----------
  addresses : list
      List of contract addresses

  Returns
  -------
  dict
      ABIs for specified addresses. Each ABI is a list.
      Each list can be empty when there is a problem with ABI extraction for this address
  """
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
  """
  Decode input data of a transaction according to a contract ABI

  Parameters
  ----------
  contract_abi : list
      List of contract methods specifications
  call_data : str
      Input of transaction in a form of 0x(4 bytes of method)(arguments),
      i.e. 0x12345678000000000000....

  Returns
  -------
  dict
      Name and parsed parameters extracted from the input
      None, if there is no such method in ABI, or there was a problem with method arguments
  """
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
  """
  Decode inputs for transactions inputs batch

  Parameters
  ----------
  encoded_params : dict
      Transaction hashes and attached tuples with contract ABI and transaction input

  Returns
  -------
  dict
      Contract addresses and attached lists of parsed parameters
  """
  return {
    hash: _decode_input(contract_abi, call_data)
    for hash, (contract_abi, call_data) in encoded_params.items()
  }

class Contracts(utils.ContractTransactionsIterator):
  _contracts_abi = {}
  doc_type = "itx"
  index = "internal_transaction"
  blocks_query = "traces_extracted:true"
  block_prefix = "inputs_decoded"

  def __init__(self, indices, host="http://localhost:9200", parity_hosts=PARITY_HOSTS):
    self.indices = indices
    self.client = CustomElasticSearch(host)
    self.pool = Pool(processes=NUMBER_OF_PROCESSES)
    self.parity_hosts = parity_hosts

  def _set_contracts_abi(self, abis):
    """Sets current contracts ABI"""
    self._contracts_abi = abis

  def _split_on_chunks(self, iterable, size):
    """
    Split given iterable onto chunks

    Parameters
    ----------
    iterable : generator
        Iterable that will be splitted
    size : int
        Max size of chunk
    Returns
    -------
    generator
        Generator that returns chunk on each iteration
    """
    return utils.split_on_chunks(iterable, size)

  def _get_contracts_abi(self, all_addresses):
    """
    Get ABI for specified contracts in parallel mode

    Parameters
    ----------
    all_addresses : list
        Contract addresses
    Returns
    -------
    list
        List of ABIs for each contract in list
    """
    chunks = self._split_on_chunks(list(enumerate(all_addresses)), NUMBER_OF_PROCESSES)
    dict_chunks = [dict(chunk) for chunk in chunks]
    abis = {key: abi for abis_dict in self.pool.map(_get_contracts_abi_sync, dict_chunks) for key, abi in abis_dict.items()}
    return list(abis.values())

  def _decode_inputs_batch(self, encoded_params):
    """
    Decode inputs in parallel mode

    Parameters
    ----------
    encoded_params : dict
        Transaction hashes and attached tuples with contract ABI and transaction input

    Returns
    -------
    dict
        Transaction hashes and parsed inputs for each transaction
    """
    chunks = list(self._split_on_chunks(list(encoded_params.items()), NUMBER_OF_PROCESSES))
    chunks = [dict(chunk) for chunk in chunks]
    decoded_inputs = self.pool.map(_decode_inputs_batch_sync, chunks)
    return {hash: input for chunk in decoded_inputs for hash, input in chunk.items()}

  def _get_range_query(self):
    """
    Get range query based on all specified blocks range in config.py

    Returns
    -------
    str
        ElasticSearch query in a form of:
        (blockNumber:[1 TO 2] OR blockNumber:[4 TO *])
    """
    ranges = [range_tuple[0:2] for range_tuple in self.parity_hosts]
    range_query = self.client.make_range_query("blockNumber", *ranges)
    return range_query

  def _iterate_contracts_without_abi(self):
    """
    Iterate through contracts without an attemp to extract ABI from etherscan.io
    within block range specified in config.py
    Returns
    -------
    generator
        Generator that iterates through contracts by conditions above
    """
    return self.client.iterate(self.indices["contract"], 'contract', 'address:* AND !(_exists_:abi_extracted) AND ' + self._get_range_query())

  def save_contracts_abi(self):
    """
    Save contracts ABI to ElasticSearch

    This function is an entry point for extract-contracts-abi operation
    """
    for contracts in self._iterate_contracts_without_abi():
      abis = self._get_contracts_abi([contract["_source"]["address"] for contract in contracts])
      operations = [self.client.update_op(doc={'abi': abis[index], 'abi_extracted': True}, id=contract["_id"]) for index, contract in enumerate(contracts)]
      self.client.bulk(operations, doc_type='contract', index=self.indices["contract"], refresh=True)

  def _iterate_contracts_with_abi(self, max_block):
    """
    Iterate through contracts with non-empty ABI
    within block range specified in config.py
    with unprocessed transactions before specified block

    Parameters
    ----------
    max_block : int
        Block number

    Returns
    -------
    generator
        Generator that iterates through contracts by conditions above
    """
    query = {
      "bool": {
        "must": [
          {"exists": {"field": "address"}},
          {"exists": {"field": "abi"}},
          {"query_string": {"query": self._get_range_query()}},
        ]
      }
    }
    return self._iterate_contracts(max_block, query)

  def _iterate_transactions_by_targets(self, contracts, max_block):
    """
    Iterate through internal CALL transactions without errors
    to specified contracts before specified block

    Parameters
    ----------
    contracts : list
        Contracts info in ElasticSearch JSON format, i.e.
        {"_id": TRANSACTION_ID, "_source": {"document": "fields"}}
    max_block : int
        Block number

    Returns
    -------
    generator
        Generator that iterates through transactions by conditions above
    """
    query = {
      "bool": {
        "must": [
          {"term": {"callType": "call"}},
        ],
        "must_not": [
          {"exists": {"field": "error"}}
        ]
      }
    }
    return self._iterate_transactions(contracts, max_block, query)

  def _decode_inputs_for_contracts(self, contracts, max_block):
    """
    Decode inputs for specified contracts before specified block

    Treats exceptions during parsing

    Parameters
    ----------
    contracts : list
        Contracts info in ElasticSearch JSON format, i.e.
        {"_id": TRANSACTION_ID, "_source": {"document": "fields"}}
    max_block : int
        Block number
    """
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
    """
    Decode inputs for all transactions to contracts with ABI in ElasticSearch

    This function is an entry point for parse-inputs operation
    """
    max_block = utils.get_max_block(self.blocks_query)
    for contracts in self._iterate_contracts_with_abi(max_block):
      self._set_contracts_abi({contract["_source"]["address"]: contract["_source"]["abi"] for contract in contracts})
      self._decode_inputs_for_contracts(contracts, max_block)
      self._save_max_block([contract["_source"]["address"] for contract in contracts], max_block)