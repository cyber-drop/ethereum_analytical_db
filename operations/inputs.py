import json
from ethereum.abi import (
    decode_abi,
    normalize_name as normalize_abi_method_name,
    method_id as get_abi_method_id)
from ethereum.utils import encode_int, zpad, decode_hex
from multiprocessing import Pool
from config import PARITY_HOSTS, INDICES, INPUT_PARSING_PROCESSES
import utils
from clients.custom_clickhouse import CustomClickhouse

NUMBER_OF_PROCESSES = INPUT_PARSING_PROCESSES

def _decode_input(contract_abi, call_data):
  """
  Decode input data of a transaction according to a contract ABI

  Solution from https://ethereum.stackexchange.com/questions/20897/how-to-decode-input-data-from-tx-using-python3?rq=1

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
      return {
        'name': method_name,
        'params.type': [arg["type"] for arg in args],
        'params.value': [arg["value"] for arg in args]
      }

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

class ClickhouseInputs(utils.ClickhouseContractTransactionsIterator):
  _contracts_abi = {}
  block_prefix = "inputs_decoded"

  def __init__(self, indices=INDICES, parity_hosts=PARITY_HOSTS):
    self.indices = indices
    self.client = CustomClickhouse()
    self.pool = Pool(processes=NUMBER_OF_PROCESSES)
    self.parity_hosts = parity_hosts

  def _set_contracts_abi(self, abis):
    """Sets current contracts ABI"""
    self._contracts_abi = {
      address: json.loads(abi)
      for address, abi in abis.items()
    }

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
    range_query = utils.make_range_query("blockNumber", *ranges)
    return range_query

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
    query = "ANY INNER JOIN {} USING id WHERE abi IS NOT NULL AND {}".format(
      self.indices["contract_abi"],
      self._get_range_query()
    )
    return self._iterate_contracts(max_block, query, fields=["abi", "address"])

  def _add_id_to_inputs(self, decoded_inputs):
    for hash, input in decoded_inputs.items():
      input.update({
        "id": hash
      })

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
            self._contracts_abi[transaction["_source"][self.contract_field]],
            transaction["_source"]["input"]
          )
          for transaction in transactions
        }
        decoded_inputs = self._decode_inputs_batch(inputs)
        self._add_id_to_inputs(decoded_inputs)
        self.client.bulk_index(index=self.indices[self.input_index], docs=list(decoded_inputs.values()))
      except Exception as exception:
        print(exception)

  def decode_inputs(self):
    """
    Decode inputs for all transactions to contracts with ABI in ElasticSearch

    This function is an entry point for parse-inputs operation
    """
    max_block = self._get_max_block({self.block_flag_name: 1})
    for contracts in self._iterate_contracts_with_abi(max_block):
      self._set_contracts_abi({contract["_source"]["address"]: contract["_source"]["abi"] for contract in contracts})
      self._decode_inputs_for_contracts(contracts, max_block)
      self._save_max_block([contract["_source"]["address"] for contract in contracts], max_block)

class ClickhouseTransactionsInputs(ClickhouseInputs):
  doc_type = "itx"
  index = "internal_transaction"
  input_index = "transaction_input"
  block_flag_name = "traces_extracted"
  contract_field = "to"

  def _iterate_transactions_by_targets(self, contracts, max_block):
    return self._iterate_transactions(contracts, max_block, "WHERE error IS NULL AND callType = 'call'", fields=["input", "to"])

class ClickhouseEventsInputs(ClickhouseInputs):
  doc_type = "event"
  index = "event"
  input_index = "event_input"
  block_flag_name = "events_extracted"
  contract_field = "address"

  def _iterate_transactions_by_targets(self, contracts, max_block):
    for transactions in self._iterate_transactions(contracts, max_block, "WHERE id IS NOT NULL", fields=["topics", "data", "address"]):
      for transaction in transactions:
        transaction = transaction["_source"]
        transaction["input"] = transaction["topics"][0][0:10] + "".join([topic[2:] for topic in transaction["topics"][1:]]) + transaction["data"][2:]
      yield transactions

