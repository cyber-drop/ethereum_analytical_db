import requests
from tqdm import *
import json
import re
from web3 import Web3, HTTPProvider
from elasticsearch import Elasticsearch, helpers

NUMBER_OF_JOBS = 1000
headers = {'Content-Type': 'application/json'}

class ContractMethods:
  def __init__(self, elasticsearch_index, 
    elasticsearch_host="http://localhost:9200", 
    ethereum_api_host="http://localhost:8545"):
    self.index = elasticsearch_index
    self.client = Elasticsearch(elasticsearch_host)
    self.elasticsearch_host = elasticsearch_host
    self.ethereum_api_host = ethereum_api_host
    self.w3 = Web3(HTTPProvider(ethereum_api_host))

  def extract_first_bytes(self, func):
    return str(self.w3.toHex(self.w3.sha3(text=func)[0:4]))[2:]

  def iterate_contracts(self, per=NUMBER_OF_JOBS):
    items_count = requests.get(self.elasticsearch_host + '/' + self.index + '/contract/_count').json()['count']
    pages = round(items_count / per + 0.4999)
    
    scroll_id = None
    scroll_url = self.elasticsearch_host + '/' + self.index + '/_search?scroll=1m'
    for page in tqdm(range(pages)):
      if not scroll_id:
        response = requests.post(scroll_url, headers=headers)
        page_content = response.json()['hits']['hits']
        scroll_id = response.json()['_scroll_id']
      else:
        scroll_body = {
          'scroll': '1m',
          'scroll_id': scroll_id
        }
        response = requests.post(self.elasticsearch_host + '/_search/scroll', headers=headers, json=scroll_body)
        page_content = response.json()['hits']['hits']
      yield page_content
  
  def search_methods(self):
    standards = {
      'erc20': {
        'totalSupply': self.extract_first_bytes('totalSupply()'),
        'balanceOf': self.extract_first_bytes('balanceOf(address)'),
        'allowance': self.extract_first_bytes('allowance(address,address)'),
        'transfer': self.extract_first_bytes('transfer(address,uint256)'),
        'transferFrom': self.extract_first_bytes('transferFrom(address,address,uint256)'),
        'approve': self.extract_first_bytes('approve(address,uint256)'),
      },
      'erc223': {
        'tokenFallback': self.extract_first_bytes('tokenFallback(address,uint256,bytes)')
      }
    }

    for contracts_chunk in self.iterate_contracts():
      for contract in contracts_chunk:
        code = self.w3.toHex(self.w3.eth.getCode(self.w3.toChecksumAddress(contract['_source']['address'])))
        avail_standards = []
        for standard in standards:
          methods = []
          for method in standards[standard]:
            res = re.search(r'' + standards[standard][method], code) != None
            methods.append(res)
          if False not in methods:
            avail_standards.append(standard)
        self.client.update(index=self.index, doc_type='contract', id=contract['_id'], body={'doc': {'standards': avail_standards}})
