from web3 import Web3, HTTPProvider
from custom_elastic_search import CustomElasticSearch
from config import INDICES
import requests
import json

class TokenHolders:
  def __init__(self, elasticsearch_indices=INDICES, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.w3 = Web3(HTTPProvider(ethereum_api_host))

  def _get_tokens_list(self):
    response = requests.get('https://api.coinmarketcap.com/v2/listings/')
    return response.json()['data']

  def _construct_msearch_body(self, names_list):
    search_arr = []
    for name in names_list:
      search_arr.append({'index': self.indices['contract'], 'type': 'contract'})
      search_arr.append({'query': {'match': {'token_name': {'query': name, 'minimum_should_match': '100%'}}}})
    body = ''
    for obj in search_arr:
      body += '%s \n' %json.dumps(obj)
    return body

  def _search_multiple_tokens(self, token_names):
    body = self._construct_msearch_body(token_names)
    response = self.client.send_request('GET', [self.indices['contract'], 'contract', '_msearch'], body, {})
    results = response['responses']
    erc_tokens = [result['hits']['hits'] for result in results if len(result['hits']['hits']) > 0]
    return erc_tokens

  def _search_duplicates(self):
    coinmarketcap_tokens = self._get_tokens_list()
    coinmarketcap_names = [token['name'] for token in coinmarketcap_tokens]
    token_contracts = self._search_multiple_tokens(coinmarketcap_names)
    duplicated_tokens = [contracts for contracts in token_contracts if len(contracts) > 1]
    return duplicated_tokens

