import re
from web3 import Web3, HTTPProvider
from custom_elastic_search import CustomElasticSearch
from config import INDICES, PARITY_HOSTS
import json
import math
from decimal import Decimal

with open('standard-token-abi.json') as json_file:
  standard_token_abi = json.load(json_file)

class ContractMethods:
  def __init__(self, elasticsearch_indices=INDICES, elasticsearch_host="http://localhost:9200", parity_hosts=PARITY_HOSTS):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.w3 = Web3(HTTPProvider(parity_hosts[0][2]))
    self.standard_token_abi = standard_token_abi
    self.standards = self._extract_methods_signatures()
    self.constants = ['name', 'symbol', 'decimals', 'total_supply', 'owner']

  def _iterate_contracts(self):
    return self.client.iterate(self.indices["contract"], 'contract', 'address:*')

  def _iterate_non_standard(self):
    return self.client.iterate(self.indices["contract"], 'contract', 'standards: None')

  def _extract_first_bytes(self, func):
    return str(self.w3.toHex(self.w3.sha3(text=func)[0:4]))[2:]

  def _extract_methods_signatures(self):
    return {
      'erc20': {
        'totalSupply': self._extract_first_bytes('totalSupply()'),
        'balanceOf': self._extract_first_bytes('balanceOf(address)'),
        'allowance': self._extract_first_bytes('allowance(address,address)'),
        'transfer': self._extract_first_bytes('transfer(address,uint256)'),
        'transferFrom': self._extract_first_bytes('transferFrom(address,address,uint256)'),
        'approve': self._extract_first_bytes('approve(address,uint256)'),
      },
      'erc223': {
        'tokenFallback': self._extract_first_bytes('tokenFallback(address,uint256,bytes)')
      }
    }

  def _check_is_token(self, bytecode):
    has_trasfer_method = re.search(r'' + self.standards['erc20']['transfer'], bytecode) != None
    return has_trasfer_method

  def _check_standards(self, bytecode):
    avail_standards = []
    for standard in self.standards:
      methods = []
      for method in self.standards[standard]:
        res = re.search(r'' + self.standards[standard][method], bytecode) != None
        methods.append(res)
      if False not in methods:
        avail_standards.append(standard)
    return avail_standards

  
  def _round_supply(self, supply, decimals):
    if decimals > 1:
      supply = supply / math.pow(10, decimals)
      supply = Decimal(supply)
      supply = round(supply)
      supply = str(supply)
    else: 
      supply = str(supply)
    return supply

  def _constant_methods(self, contract_instance):
    return {
      'name': {'func': contract_instance.functions.name(), 'placeholder': 'None'},
      'symbol': {'func': contract_instance.functions.symbol(), 'placeholder': 'None'},
      'decimals': {'func': contract_instance.functions.decimals(),'placeholder': 1},
      'total_supply': {'func': contract_instance.functions.totalSupply(),'placeholder': '0'},
      'owner': {'func': contract_instance.functions.owner(), 'placeholder': 'None'}
    }

  def _get_constants(self, address):
    contract_checksum_addr = self.w3.toChecksumAddress(address)
    contract_instance = self.w3.eth.contract(address=contract_checksum_addr, abi=self.standard_token_abi)
    methods = self._constant_methods(contract_instance)
    contract_constants = []
    for constant in self.constants:
      try:
        response = methods[constant]['func'].call()
      except:
        response = methods[constant]['placeholder']
      if constant == 'total_supply' and response != '0':
        response = self._round_supply(response, contract_constants[2])
      contract_constants.append(response)
    return contract_constants
    
  def _update_contract_descr(self, doc_id, body):
    self.client.update(self.indices['contract'], 'contract', doc_id, doc=body, refresh=True)

  def _classify_contract(self, contract):
    code = contract['_source']['bytecode']
    is_token = self._check_is_token(code)
    if is_token == True:
      token_standards = self._check_standards(code)
      if len(token_standards) > 0:
        name, symbol, decimals, total_supply, owner = self._get_constants(contract['_source']['address'])
        update_body = {'standards': token_standards, 'token_name': name, 'token_symbol': symbol, 'decimals': decimals, 'total_supply': total_supply, 'token_owner': owner, 'is_token': True}
        self._update_contract_descr(contract['_id'], update_body)
      else:
        update_body = {'standards': ['None'], 'is_token': True}
        self._update_contract_descr(contract['_id'], update_body)
    else:
      update_body = {'is_token': False}
      self._update_contract_descr(contract['_id'], update_body)
  
  def search_methods(self):
    for contracts_chunk in self._iterate_contracts():
      for contract in contracts_chunk:
        self._classify_contract(contract)
    for tokens_chunk in self._iterate_non_standard():
      for token in tokens_chunk:
        name, symbol, decimals, total_supply, owner = self._get_constants(token['_source']['address'])
        update_body = {'token_name': name, 'token_symbol': symbol, 'decimals': decimals, 'total_supply': total_supply, 'token_owner': owner}
        self._update_contract_descr(token['_id'], update_body)