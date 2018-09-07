from web3 import Web3
from custom_elastic_search import CustomElasticSearch
from config import INDICES
import requests
from pyelasticsearch import bulk_chunks
import math
import json
import re
import pdb
import utils

ADDRESS_ENCODING_CONSTANT = 0x0010000000000000000000000000000000000000000

class TokenHolders(utils.ContractTransactionsIterator):
  '''
  Extract information about token transfers from internal txs and save it in a separate token_tx index
  
  Parameters
  ----------
  elasticsearch_indices (dict): Dictionary containing exisiting Elasticsearch indices
  elasticsearch_host (str): Elasticsearch url
  '''
  def __init__(self, elasticsearch_indices=INDICES, elasticsearch_host="http://localhost:9200"):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.token_decimals = {}
    self.w3 = Web3()
    self.signatures = {
      # ERC-20 methods
      'transfer(address,uint256)': self._process_address_uint_tx,
      'transferFrom(address,address,uint256)': self._process_two_addr_tx,
      # TOP-20 contracts
      'burnTokens(uint256)': self._process_only_uint_negative,
      'mint(address,uint256,bool,uint32)': self._process_address_uint_tx,
      'mint(uint128)': self._process_only_uint,
      'mint(uint256)': self._process_only_uint,
      'unfreeze(uint256)': self._process_only_uint,
      'burn(uint128)': self._process_only_uint_negative,
      'burn(uint256)': self._process_only_uint_negative,
      'freeze(uint256)': self._process_only_uint_negative,
      'push(address,uint256)': self._process_address_uint_tx,
      'push(address,uint128)': self._process_address_uint_tx,
      'pull(address,uint256)': self._process_address_uint_reversed_tx,
      'pull(address,uint128)': self._process_address_uint_reversed_tx,
      'move(address,address,uint256)': self._process_two_addr_tx,
      # TOP-50 contracts
      'transferForMultiAddresses(address[],unit256[])': self._process_multiple_addr_tx,
      'mintTokensWithinTime(address,uint256)': self._process_address_uint_tx,
      'issue(address,uint256)': self._process_address_uint_tx,
      'destroy(address,uint256)': self._process_address_uint_negative_tx,
      'controllerTransfer(address,address,uint256)': self._process_two_addr_tx,
      'mintBIX(address,uint256,uint256,uint256)': self._process_address_uint_tx,
      'mintToken(address,uint256)': self._process_address_uint_tx,
      'anailNathrachOrthaBhaisIsBeathaDoChealDeanaimh(address[],uint256[])': self._process_multiple_addr_tx,
      'transfers(address[],uint256[])': self._process_multiple_addr_tx,
      'mintTokens(address,uint256)': self._process_address_uint_tx,
      'sell(uint256)': self._process_only_uint_negative,
      'sellDentacoinsAgainstEther(uint256)': self._process_only_uint_negative,
  }

  def _construct_bulk_insert_ops(self, docs):
    '''
    Iterate over docs and create document-inserting operations used in bulk insert

    Parameters
    ----------
    docs: list
      List of dictionaries with new data
    '''
    for doc in docs:
      yield self.client.index_op(doc, id=doc['tx_hash'])

  def _construct_bulk_update_ops(self, docs):
    '''
    Iterate over docs and create document-updating operations used in bulk update

    Parameters
    ----------
    docs: list
      List of dictionaries with new data
    '''
    for doc in docs:
      yield self.client.update_op(doc['doc'], id=doc['id'])

  def _insert_multiple_docs(self, docs, doc_type, index_name):
    ''' 
    Index multiple documents simultaneously

    Parameters
    ----------
    docs: list
      List of dictionaries with new data
    doc_type: str 
      Type of inserted documents
    index_name: str
      Name of the index that contains inserted documents
    '''
    for chunk in bulk_chunks(self._construct_bulk_insert_ops(docs), docs_per_chunk=1000):
      self.client.bulk(chunk, doc_type=doc_type, index=index_name, refresh=True)

  def _update_multiple_docs(self, docs, doc_type, index_name):
    '''
    Update multiple documents simultaneously

    Parameters
    ----------
    docs: list
      List of dictionaries with new data
    doc_type: str 
      Type of updated documents
    index_name: str
      Name of the index that contains updated documents
    '''
    for chunk in bulk_chunks(self._construct_bulk_update_ops(docs), docs_per_chunk=1000):
      self.client.bulk(chunk, doc_type=doc_type, index=index_name, refresh=True)

  def _iterate_tokens(self, max_block):
    '''
    Iterate over token contracts that is listed on Coinmarketcap
    
    Parameters
    ----------
    max_block: int
      Block upper limit

    Returns
    -------
    generator
      Generator that iterates over token contracts in Elasticsearch
    '''
    query = {
      "query_string": {
        "query": '_exists_:cmc_id'
      }
    }
    return self._iterate_contracts(max_block, query)

  def _iterate_tokens_txs(self, tokens, max_block):
    '''
    Iterate over internal txs that were sent to token contracts

    Parameters
    ----------
    tokens: list
      List of token addresses
    max_block: int 
      Block upper limit
    
    Returns
    -------
    generator
      Generator that iterates over internal transactions in Elasticsearch
    '''
    query = {
      "query_string": {
        "query": "*"
      }
    }
    return self._iterate_transactions(tokens, max_block, query)

  def _check_decimals(self, token):
    '''
    Check is decimals variable is specified in contract. If not - use placeholder (18)
    
    Parameters
    ----------
    token: str
      Token address

    Returns
    -------
    dec: int
      Token decimals
    '''
    dec = self.token_decimals[token] if token in self.token_decimals.keys() else 18
    return dec

  def _convert_transfer_value(self, value, decimals):
    '''
    Subtract decimals from transfer value
    
    Parameters
    ----------
    value: int
      Transfer value
    decimals:int
      Token decimals

    Returns
    -------
    rounded: float
      Transfer value without decimals
    '''
    value = int(value)
    rounded = value / math.pow(10, decimals)
    rounded = "{0:.5f}".format(rounded)
    return (rounded, float(rounded))

  def _construct_signature(self, inputs):
    '''
    Create string that contains function name and parameters and is used as function signature

    Parameters
    ----------
    inputs: dict
      A dictionary with decoded transaction input

    Returns
    -------
    signture: str
      A string containing function name and params
    '''
    method = inputs['name']
    params = [param['type'] for param in inputs['params']]
    params = ','.join(params)
    method += '(' + params + ')'
    signature = method
    return signature

  def _check_is_valid(self, tx):
    '''
    Check does tx dict contain signs of invalid transaction
    
    Parameters
    ----------
    tx: dict
      A dictionary with transaction info

    Returns
    -------
    bool
      A boolean value signifies is tx valid
    '''
    if 'error' in tx.keys():
      return False
    elif 'output' in tx.keys() and tx['output'] == '0x0000000000000000000000000000000000000000000000000000000000000000':
      return False
    elif 'parent_error' in tx.keys() and tx['parent_error'] == True:
      return False
    else:
      return True

  def _process_only_address(self, tx):
    '''
    Convert txs whose input doesn't contain information about transfered amount of tokens

    Parameters
    ----------
    tx: dict
      A dictionary with transaction info

    Returns
    -------
    list:
      A list containing dictionary with token tx data
    '''
    tx_input = tx['decoded_input']
    valid = self._check_is_valid(tx)
    return [{
      'method': tx_input['name'], 
      'from': tx['from'],
      'block_id': tx['blockNumber'], 
      'valid': valid, 
      'token': tx['to'], 
      'tx_index': self.indices[self.tx_index], 
      'tx_hash': tx['id']
    }]

  def _process_only_uint(self, tx):
    '''
    Convert txs whose input doesn't contain information about destination of transfer

    Parameters
    ----------
    tx: dict
      A dictionary with transaction info

    Returns
    -------
    list:
      A list containing dictionary with token tx data
    '''
    tx_input = tx['decoded_input']
    decimals = self._check_decimals(tx['to'])
    value = self._convert_transfer_value(tx_input['params'][0]['value'], decimals)
    valid = self._check_is_valid(tx)
    return [{
      'method': tx_input['name'], 
      'from': tx['from'],
      'value': value[1], 
      'raw_value': value[0], 
      'block_id': tx['blockNumber'], 
      'valid': valid, 
      'token': tx['to'], 
      'tx_index': self.indices[self.tx_index], 
      'tx_hash': tx['id']
    }]

  def _process_only_uint_negative(self, tx):
    '''
    Convert txs that subtract tokens from sender and don't send it to anyone

    Parameters
    ----------
    tx: dict 
      A dictionary with transaction info

    Returns
    -------
    list:
      A list containing dictionary with token tx data
    '''
    transaction = self._process_only_uint(tx)[0]
    try:
      transaction["value"] = -transaction["value"]
    except:
      print("Exception!")
    transaction["raw_value"] = "-" + transaction["raw_value"]
    return [transaction]

  def _process_address_uint_tx(self, tx):
    '''
    Convert txs that transfer values from one sender to another address

    Parameters
    ----------
    tx: dict 
      A dictionary with transaction info

    Returns
    -------
    list:
      A list containing dictionary with token tx data
    '''
    tx_input = tx['decoded_input']
    decimals = self._check_decimals(tx['to'])
    value = self._convert_transfer_value(tx_input['params'][1]['value'], decimals)
    valid = self._check_is_valid(tx)
    return [{
      'method': tx_input['name'], 
      'from': tx['from'], 
      'to': tx_input['params'][0]['value'], 
      'value': value[1], 
      'raw_value': value[0], 
      'block_id': tx['blockNumber'], 
      'valid': valid, 
      'token': tx['to'], 
      'tx_index': self.indices[self.tx_index], 
      'tx_hash': tx['id']
    }]

  def _process_address_uint_negative_tx(self, tx):
    '''
    Convert txs that subtract tokens from specified address and don't send it to anyone

    Parameters
    ----------
    tx: dict 
      A dictionary with transaction info

    Returns
    -------
    list:
      A list containing dictionary with token tx data
    '''
    transaction = self._process_address_uint_tx(tx)[0]
    try:
      transaction["value"] = -transaction["value"]
    except:
      print("Exception!")
    transaction["raw_value"] = "-" + transaction["raw_value"]
    return [transaction]

  '''
  def _process_uint_address_tx(self, tx):
    tx['decoded_input']['params'] = list(reversed(tx['decoded_input']['params']))
    return self._process_address_uint_tx(tx)
  '''

  def _process_two_addr_tx(self, tx):
    '''
    Convert txs that transfer values from one address (which is not tx sender) to another

    Parameters
    ----------
    tx: dict 
      A dictionary with transaction info

    Returns
    -------
    list:
      A list containing dictionary with token tx data
    '''
    tx_input = tx['decoded_input']
    decimals = self._check_decimals(tx['to'])
    value = self._convert_transfer_value(tx_input['params'][2]['value'], decimals)
    valid = self._check_is_valid(tx)
    return [{
      'method': tx_input['name'], 
      'from': tx_input['params'][0]['value'], 
      'to': tx_input['params'][1]['value'], 
      'value': value[1], 
      'raw_value': value[0], 
      'block_id': tx['blockNumber'], 
      'valid': valid, 
      'token': tx['to'], 
      'tx_index': self.indices[self.tx_index], 
      'tx_hash': tx['id']
    }]

  def _process_multiple_addr_tx(self, tx):
    '''
    Convert txs that have multiple transfers packed in one input

    Parameters
    ----------
    tx: dict 
      A dictionary with transaction info

    Returns
    -------
    list:
      A list containing dictionaries with token tx data
    '''
    tx_input = tx['decoded_input']
    decimals = self._check_decimals(tx['to'])
    addresses = re.sub('\'', '\"', tx_input['params'][0]['value'])
    addresses = json.loads(addresses)
    values = [str(value) for value in json.loads(tx_input['params'][1]['value'])]
    params = list(zip(addresses, values))
    descriptions = []
    valid = self._check_is_valid(tx)
    for i, param in enumerate(params):
      value = self._convert_transfer_value(param[1], decimals)
      descr = {
        'method': tx_input['name'], 
        'from': tx['from'], 
        'to': param[0], 
        'value': value[1], 
        'raw_value': value[0], 
        'block_id': tx['blockNumber'], 
        'valid': valid, 
        'token': tx['to'], 
        'tx_index': self.indices[self.tx_index], 
        'tx_hash': tx['id'] + '_' + str(i)
      }
      descriptions.append(descr)
    return descriptions

  def _process_multi_addr_one_uint(self, tx):
    '''
    Convert txs that have multiple transfers with same value packed in one input

    Parameters
    ----------
    tx: dict 
      A dictionary with transaction info

    Returns
    -------
    list:
      A list containing dictionaries with token tx data
    '''
    tx_input = tx['decoded_input']
    decimals = self._check_decimals(tx['to'])
    addresses = re.sub('\'', '\"', tx_input['params'][0]['value'])
    addresses = json.loads(addresses)
    values = [str(tx_input['params'][1]['value']) for i in range(len(addresses))]
    params = list(zip(addresses, values))
    descriptions = []
    valid = self._check_is_valid(tx)
    for i, param in enumerate(params):
      value = self._convert_transfer_value(param[1], decimals)
      descr = {
        'method': tx_input['name'],
        'from': tx['from'], 
        'to': param[0], 
        'value': value[1], 
        'raw_value': value[0], 
        'block_id': tx['blockNumber'], 
        'valid': valid, 
        'token': tx['to'], 
        'tx_index': self.indices[self.tx_index], 
        'tx_hash': tx['id'] + '_' + str(i)
      }
      descriptions.append(descr)
    return descriptions
  
  def _process_address_uint_reversed_tx(self, tx):
    '''
    Process txs that have reverse order of input params: (uint, address) instead of (address, uint)

    Parameters
    ----------
    tx: dict 
      A dictionary with transaction info

    Returns
    -------
    function:
      _process_two_addr_tx method described above
    '''
    tx["decoded_input"]['params'] = [
      tx["decoded_input"]['params'][0], 
      {"value": tx["from"]},
      tx["decoded_input"]['params'][1]
    ]
    return self._process_two_addr_tx(tx)

  def _construct_tx_descr_from_input(self, tx):
    '''
    Check is tx method signature has a handler; if true, pass transaction to this handler; else return None

    Parameters
    ----------
    tx: dict 
      A dictionary with transaction info

    Returns
    -------
    function:
      A method that handles appropriate type of txs
    '''
    method_signature = self._construct_signature(tx['decoded_input'])
    if method_signature in self.signatures:
      return self.signatures[method_signature](tx)
    else:
      return

  def _check_tx_input(self, tx):
    '''
    Check is tx has decoded input; if true, pass tx to _construct_tx_descr_from_input method; else return None

    Parameters
    ----------
    tx: dict 
      A dictionary with transaction info

    Returns
    -------
    function:
      _construct_tx_descr_from_input method described above
    '''
    if 'decoded_input' in tx['_source'].keys() and tx['_source']['decoded_input'] != None and len(tx['_source']['decoded_input']['params']) > 0:
      tx['_source']['id'] = tx['_id']
      return self._construct_tx_descr_from_input(tx['_source'])
    else:
      return

  def _extract_descriptions_from_txs(self, txs):
    '''
    Extract token tx descriptions from internal txs by methods described above and upload description in Elasticsearch

    Parameters
    ----------
    txs: list 
      A list of internal txs that contains information about token transfers
    '''
    txs_info = [self._check_tx_input(tx) for tx in txs]
    txs_info = [tx for tx in txs_info if tx != None]
    txs_info = [tx for txs in txs_info for tx in txs]
    self._insert_multiple_docs(txs_info, 'tx', self.indices['token_tx'])

  def _iterate_token_tx_descriptions(self, token_address):
    '''

    '''
    return self.client.iterate(self.indices['token_tx'], 'tx', 'token:' + token_address)

  def _iterate_tx_descriptions(self):
    '''
    Iterate over token tx descriptions

    This method is used only in tests

    Returns
    -------
    generator
      Generator that iterates over token tx descriptions in Elasticsearch
    '''
    return self.client.iterate(self.indices['token_tx'], 'tx', 'token:*')

  def _extract_tokens_txs(self, tokens, max_block):
    '''
    Iterate over internal txs and extract token tx descriptions

    Parameters
    ----------
    txs: list 
      A list of internal txs that contains information about token transfers
    max_block: int
      Block upper limit
    '''
    for txs_chunk in self._iterate_tokens_txs(tokens, max_block):
      self._extract_descriptions_from_txs(txs_chunk)

  def _construct_creation_descr(self, contract):
    '''
    Create token tx description that contains info about transfer of token supply to the token owner/creator

    This method is used to prevent negative balances in API

    Parameters
    ----------
    contract: dict 
      A dictionary with token contract info

    Returns
    -------
    dict:
      A dictionary containing data about supply transfer
    '''
    if 'token_owner' in contract.keys() and contract['token_owner'] != 'None':
      to = contract['token_owner']
    elif 'owner' in contract.keys():
      to = contract['owner']
    else:
      to = contract['creator']
    value = contract['total_supply'] if 'total_supply' in contract.keys() and contract['total_supply'] != 'None' else '0'
    transaction_index = self.indices['internal_transaction']
    return {
      'method': 'initial', 
      'to': to, 
      'raw_value': value,
      'value': int(value), 
      'block_id': contract['blockNumber'], 
      'valid': True, 
      'token': contract['address'],
      'tx_index': transaction_index, 
      'tx_hash': contract['address']
    }

  def _extract_contract_creation_descr(self, contracts):
    '''
    Iterate over contracts and create supply transfer descriptions

    Parameters
    ----------
    contracts: list 
      A list with token contracts info
    '''
    descriptions = [self._construct_creation_descr(contract['_source']) for contract in contracts]
    self._insert_multiple_docs(descriptions, 'tx', self.indices['token_tx'])

  def get_listed_tokens_txs(self):
    '''
    Extract list of tokens listed on Coinmarketcap and create token tx descriptions

    This function is an entry point for extract-token-transactions operation
    '''
    max_block = utils.get_max_block()
    for tokens in self._iterate_tokens(max_block):
      self.token_decimals = {token['_source']['address']: token['_source']['decimals'] for token in tokens if 'decimals' in token['_source'].keys()}
      self._extract_contract_creation_descr(tokens)
      self._extract_tokens_txs(tokens, max_block)
      self._save_max_block([token["_source"]["address"] for token in tokens], max_block)

  def _get_listed_tokens_addresses(self):
    '''
    Get tokens listed on Coinmarketcap and extract list of addresses

    Returns
    -------
    list:
      A list containing listed token addresses
    '''
    addresses = []
    for tokens in self._iterate_tokens():
      for token in tokens:
        addresses.append(token['_source']['address'])
    return addresses

  def run(self, block):
    '''
    Get txs included in block and extract token tx descriptions
    
    Parameters
    ----------
    block: int 
      block number
    '''
    listed_tokens_addresses = self._get_listed_tokens_addresses()
    transfer_methods = ['transfer', 'transferFrom', 'approve']
    transfers = []
    for tx in block['transactions']:
      if tx['to'] in listed_tokens_addresses and tx['decoded_input']['name'] in transfer_methods:
        tx_descr = self._construct_tx_descr_from_input(tx)
        transfers.append(tx_descr)
    self._insert_multiple_docs(transfers, 'tx', self.indices['token_tx'])

class InternalTokenTransactions(TokenHolders):
  tx_index = 'internal_transaction'
  index = 'internal_transaction'
  tx_type = 'itx'
  doc_type = 'itx'
  block_prefix = 'token_transactions_extracted'
