from web3 import Web3, HTTPProvider
from custom_elastic_search import CustomElasticSearch
from config import INDICES
import requests
import json
from pyelasticsearch import bulk_chunks

class TokenHolders:
  def __init__(self, elasticsearch_indices=INDICES, elasticsearch_host="http://localhost:9200", tx_index='internal'):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.tx_index = self.indices['internal_transaction'] if tx_index == 'internal' else self.indices['transaction']
    self.tx_type = 'itx' if tx_index == 'internal' else 'tx'

  def _construct_bulk_insert_ops(self, docs):
    for doc in docs:
      yield self.client.index_op(doc)

  def _insert_multiple_docs(self, docs, doc_type, index_name):
    for chunk in bulk_chunks(self._construct_bulk_insert_ops(docs), docs_per_chunk=1000):
      self.client.bulk(chunk, doc_type=doc_type, index=index_name, refresh=True)

  def _iterate_tokens(self):
    return self.client.iterate(self.indices['contract'], 'contract', 'cmc_listed:true')

  def _iterate_tokens_txs(self, token_addresses):
    query = {
      "terms": {
        "to": token_addresses
      }
    }
    return self.client.iterate(self.tx_index, self.tx_type, query)

  def _construct_tx_descr_from_input(self, tx):
    tx_input = tx['decoded_input']
    is_valid_tx = 'error' not in tx.keys()
    if tx_input['name'] == 'transfer':
      return {'method': tx_input['name'], 'from': tx['from'], 'to': tx_input['params'][0]['value'], 'value': tx_input['params'][1]['value'],'block_id': tx['blockNumber'], 'valid': is_valid_tx, 'token': tx['to'], 'tx_index': self.indices['internal_transaction']}
    elif tx_input['name'] == 'transferFrom':
      return {'method': tx_input['name'], 'from': tx_input['params'][0]['value'], 'to': tx_input['params'][1]['value'], 'value': tx_input['params'][2]['value'], 'block_id': tx['blockNumber'], 'valid': is_valid_tx, 'token': tx['to'], 'tx_index': self.indices['internal_transaction']}
    elif tx_input['name'] == 'approve':
      return {'method': tx_input['name'], 'from': tx['from'], 'spender': tx_input['params'][0]['value'], 'value': tx_input['params'][1]['value'],'block_id': tx['blockNumber'], 'valid': is_valid_tx, 'token': tx['to'], 'tx_index': self.indices['internal_transaction']}
    else:
      return

  def _check_tx_input(self, tx):
    if 'decoded_input' in tx['_source'].keys() and tx['_source']['decoded_input'] != None:
      return self._construct_tx_descr_from_input(tx['_source'])
    else:
      return

  def _extract_descriptions_from_txs(self, txs):
    txs_info = [self._check_tx_input(tx) for tx in txs]
    txs_info = [tx for tx in txs_info if tx != None]
    self._insert_multiple_docs(txs_info, 'tx', self.indices['token_tx'])

  def _iterate_token_tx_descriptions(self, token_address):
    return self.client.iterate(self.indices['token_tx'], 'tx', 'token:' + token_address)

  def _iterate_tx_descriptions(self):
    return self.client.iterate(self.indices['token_tx'], 'tx', 'token:*')

  def _extract_tokens_txs(self, token_addresses):
    for txs_chunk in self._iterate_tokens_txs(token_addresses):
      self._extract_descriptions_from_txs(txs_chunk) 

  def get_listed_tokens_txs(self):
    for tokens in self._iterate_tokens():
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

