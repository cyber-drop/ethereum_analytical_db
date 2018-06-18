from web3 import Web3
from custom_elastic_search import CustomElasticSearch
from config import INDICES
import requests
from pyelasticsearch import bulk_chunks
import math
import json
import re

class TokenHolders:
  def __init__(self, elasticsearch_indices=INDICES, elasticsearch_host="http://localhost:9200"):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.token_decimals = {}
    self.w3 = Web3()
    self.signatures = {
      'transfer(address,uint256)': self._process_address_uint_tx,
      'approve(address,uint256)': self._process_address_uint_tx,
      'transferFrom(address,address,uint256)': self._process_two_addr_tx,
      'multiTransfer(address[],uint256[])': self._process_multiple_addr_tx,
      'burnTokens': self._process_only_uint,
      'prefill(address[],uint256[])': self._process_multiple_addr_tx,
      'generateTokens(address,uint256)': self._process_address_uint_tx,
      'destroyTokens(address,uint256)': self._process_address_uint_tx,
      'claimTokens(address)': self._process_only_address,
      'mint(address,uint256)': self._process_address_uint_tx,
      'mintToAddressesAndAmounts': self._process_multiple_addr_tx,
      'mintToAddresses': self._process_multi_addr_one_uint
    }

  def _construct_bulk_insert_ops(self, docs):
    for doc in docs:
      yield self.client.index_op(doc, id=doc['tx_hash'])

  def _construct_bulk_update_ops(self, docs):
    for doc in docs:
      yield self.client.update_op(doc['doc'], id=doc['id'])

  def _insert_multiple_docs(self, docs, doc_type, index_name):
    for chunk in bulk_chunks(self._construct_bulk_insert_ops(docs), docs_per_chunk=1000):
      self.client.bulk(chunk, doc_type=doc_type, index=index_name, refresh=True)

  def _update_multiple_docs(self, docs, doc_type, index_name):
    for chunk in bulk_chunks(self._construct_bulk_update_ops(docs), docs_per_chunk=1000):
      self.client.bulk(chunk, doc_type=doc_type, index=index_name, refresh=True)

  def _iterate_tokens(self):
    return self.client.iterate(self.indices['contract'], 'contract', '_exists_:cmc_id AND !(tx_descr_scanned:true)')

  def _iterate_tokens_txs(self, token_addresses):
    query = {
      "terms": {
        "to": token_addresses
      }
    }
    return self.client.iterate(self.indices[self.tx_index], self.tx_type, query)

  def _convert_transfer_value(self, value, decimals):
    if decimals == 1:
      return (value, None)
    value = int(value)
    rounded = value / math.pow(10, decimals)
    rounded = "{0:.5f}".format(rounded)
    return (rounded, float(rounded))

  def _extract_first_bytes(self, func):
    return str(self.w3.toHex(self.w3.sha3(text=func)))[2:]

  def _construct_signature(self, inputs):
    method = inputs['name']
    params = [param['type'] for param in inputs['params']]
    params = ','.join(params)
    method += '(' + params + ')'
    #signature = self._extract_first_bytes(method)
    signature = method
    return signature

  def _check_is_valid(self, tx):
    if 'error' in tx.keys():
      return False
    elif 'output' in tx.keys() and tx['output'] == '0x0000000000000000000000000000000000000000000000000000000000000000':
      return False
    else:
      return True

  def _process_only_address(self, tx):
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
    tx_input = tx['decoded_input']
    decimals = self.token_decimals[tx['to']] if tx['to'] in self.token_decimals.keys() else 1
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
    
  def _process_address_uint_tx(self, tx):
    tx_input = tx['decoded_input']
    decimals = self.token_decimals[tx['to']] if tx['to'] in self.token_decimals.keys() else 1
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

  def _process_two_addr_tx(self, tx):
    tx_input = tx['decoded_input']
    decimals = self.token_decimals[tx['to']] if tx['to'] in self.token_decimals.keys() else 1
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
    tx_input = tx['decoded_input']
    decimals = self.token_decimals[tx['to']] if tx['to'] in self.token_decimals.keys() else 1
    addresses = re.sub('\'', '\"', tx_input['params'][0]['value'])
    addresses = json.loads(addresses)
    values = [str(value) for value in json.loads(tx_input['params'][1]['value'])]
    params = list(zip(addresses, values))
    print(params)
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
    tx_input = tx['decoded_input']
    decimals = self.token_decimals[tx['to']] if tx['to'] in self.token_decimals.keys() else 1
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
    
  def _construct_tx_descr_from_input(self, tx):
    method_signature = self._construct_signature(tx['decoded_input'])
    if method_signature in self.signatures:
      return self.signatures[method_signature](tx)
    else:
      return

  def _check_tx_input(self, tx):
    if 'decoded_input' in tx['_source'].keys() and tx['_source']['decoded_input'] != None and len(tx['_source']['decoded_input']['params']) > 0:
      tx['_source']['id'] = tx['_id']
      return self._construct_tx_descr_from_input(tx['_source'])
    else:
      return

  def _extract_descriptions_from_txs(self, txs):
    txs_info = [self._check_tx_input(tx) for tx in txs]
    txs_info = [tx for tx in txs_info if tx != None]
    txs_info = [tx for txs in txs_info for tx in txs]
    self._insert_multiple_docs(txs_info, 'tx', self.indices['token_tx'])

  def _iterate_token_tx_descriptions(self, token_address):
    return self.client.iterate(self.indices['token_tx'], 'tx', 'token:' + token_address)

  def _iterate_tx_descriptions(self):
    return self.client.iterate(self.indices['token_tx'], 'tx', 'token:*')

  def _extract_tokens_txs(self, token_addresses):
    for txs_chunk in self._iterate_tokens_txs(token_addresses):
      self._extract_descriptions_from_txs(txs_chunk)
    update_docs = [{'doc': {'tx_descr_scanned': True}, 'id': address} for address in token_addresses]
    self._update_multiple_docs(update_docs, 'contract', self.indices['contract'])

  def _construct_creation_descr(self, contract):
    if 'token_owner' in contract.keys() and contract['token_owner'] != 'None':
      to = contract['token_owner']
    elif 'owner' in contract.keys():
      to = contract['owner']
    else:
      to = contract['creator']
    value = contract['total_supply'] if 'total_supply' in contract.keys() and contract['total_supply'] != 'None' else '0'
    transaction_index = self.indices['transaction'] if re.search(r'\.', contract['parent_transaction']) == None else self.indices['internal_transaction']
    return {
      'method': 'initial', 
      'to': to, 
      'raw_value': value,
      'value': int(value), 
      'block_id': contract['blockNumber'], 
      'valid': True, 
      'token': contract['address'],
      'tx_index': transaction_index, 
      'tx_hash': contract['parent_transaction'] if 'parent_transaction' in contract.keys() else contract['address']
    }

  def _extract_contract_creation_descr(self, contracts):
    descriptions = [self._construct_creation_descr(contract['_source']) for contract in contracts]
    self._insert_multiple_docs(descriptions, 'tx', self.indices['token_tx'])

  def get_listed_tokens_txs(self):
    for tokens in self._iterate_tokens():
      self.token_decimals = {token['_source']['address']: token['_source']['decimals'] for token in tokens if 'decimals' in token['_source'].keys()}
      self._extract_contract_creation_descr(tokens)
      self._extract_tokens_txs([token['_source']['address'] for token in tokens])

  def _get_listed_tokens_addresses(self):
    addresses = []
    for tokens in self._iterate_tokens():
      for token in tokens:
        addresses.append(token['_source']['address'])
    return addresses

  def run(self, block):
    listed_tokens_addresses = self._get_listed_tokens_addresses()
    transfer_methods = ['transfer', 'transferFrom', 'approve']
    transfers = []
    for tx in block['transactions']:
      if tx['to'] in listed_tokens_addresses and tx['decoded_input']['name'] in transfer_methods:
        tx_descr = self._construct_tx_descr_from_input(tx)
        transfers.append(tx_descr)
    self._insert_multiple_docs(transfers, 'tx', self.indices['token_tx'])

class ExternalTokenTransactions(TokenHolders):
  tx_index = 'transaction'
  tx_type = 'tx'

class InternalTokenTransactions(TokenHolders):
  tx_index = 'internal_transaction'
  tx_type = 'itx'
