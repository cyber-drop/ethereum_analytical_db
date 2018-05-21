from tqdm import *
import re
from web3 import Web3, HTTPProvider
from custom_elastic_search import CustomElasticSearch
from config import INDICES
import json
import math
from decimal import Decimal

with open('standard-token-abi.json') as json_file:
  standard_token_abi = json.load(json_file)

class ContractMethods:
  def __init__(self, elasticsearch_indices=INDICES, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.w3 = Web3(HTTPProvider(ethereum_api_host))
    self.standard_token_abi = standard_token_abi
    self.standards = self._extract_methods_signatures()

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

  def _get_contract_bytecode(self, address):
    contract_checksum_addr = self.w3.toChecksumAddress(address)
    contract_code_bytearr = self.w3.eth.getCode(contract_checksum_addr)
    return self.w3.toHex(contract_code_bytearr)

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

  def _get_constants(self, address):
    contract_checksum_addr = self.w3.toChecksumAddress(address)
    contract_instance = self.w3.eth.contract(address=contract_checksum_addr, abi=self.standard_token_abi)
    try:
      name = contract_instance.functions.name().call()
    except:
      name = 'None'
    try:
      symbol = contract_instance.functions.symbol().call()
    except:
      symbol = 'None'
    try:
      decimals = contract_instance.functions.decimals().call()
    except:
      decimals = 1
    try:
      total_supply = contract_instance.functions.totalSupply().call()
      total_supply = total_supply / math.pow(10, decimals)
      total_supply = Decimal(total_supply)
      total_supply = round(total_supply)
    except:
      total_supply = 0
    try:
      owner = contract_instance.functions.owner().call()
    except:
      owner = 'None'
    return (name, symbol, decimals, total_supply, owner)
    
  def _update_contract_descr(self, doc_id, body):
    self.client.update(self.indices['contract'], 'contract', doc_id, doc=body, refresh=True)

  def _classify_contract(self, contract):
    code = self._get_contract_bytecode(contract['_source']['address'])
    is_token = self._check_is_token(code)
    if is_token == True:
      token_standards = self._check_standards(code)
      if len(token_standards) > 0:
        name, symbol, decimals, total_supply, owner = self._get_constants(contract['_source']['address'])
        update_body = {'standards': token_standards, 'bytecode': code, 'token_name': name, 'token_symbol': symbol, 'decimals': decimals, 'total_supply': total_supply, 'owner': owner, 'is_token': True}
        self._update_contract_descr(contract['_id'], update_body)
      else:
        update_body = {'standards': ['None'], 'bytecode': code, 'is_token': True}
        self._update_contract_descr(contract['_id'], update_body)
    else:
      update_body = {'is_token': False, 'bytecode': code}
      self._update_contract_descr(contract['_id'], update_body)
  
  def search_methods(self):
    for contracts_chunk in self._iterate_contracts():
      for contract in contracts_chunk:
        self._classify_contract(contract)
    for tokens_chunk in self._iterate_non_standard():
      for token in tokens_chunk:
        name, symbol, decimals, total_supply, owner = self._get_constants(token['_source']['address'])
        update_body = {'token_name': name, 'token_symbol': symbol, 'decimals': decimals, 'total_supply': total_supply, 'owner': owner}
        self._update_contract_descr(token['_id'], update_body)