from tqdm import *
import re
from web3 import Web3, HTTPProvider
from custom_elastic_search import CustomElasticSearch
import json

NUMBER_OF_JOBS = 1000

class ContractMethods:
  def __init__(self, elasticsearch_index, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.index = elasticsearch_index
    self.client = CustomElasticSearch(elasticsearch_host)
    self.w3 = Web3(HTTPProvider(ethereum_api_host))

  def _extract_first_bytes(self, func):
    return str(self.w3.toHex(self.w3.sha3(text=func)[0:4]))[2:]

  def _iterate_contracts(self):
    return self.client.iterate(self.index, 'contract', 'address:*', paginate=True)

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
  def _get_standard_token_abi(self):
    with open('standard-token-abi.json') as json_file:
      standard_token_abi = json.load(json_file)
    return standard_token_abi
  def _check_standards(self, bytecode, standards):
    avail_standards = []
    for standard in standards:
      methods = []
      for method in standards[standard]:
        res = re.search(r'' + standards[standard][method], bytecode) != None
        methods.append(res)
      if False not in methods:
        avail_standards.append(standard)
    return avail_standards
  def _search_constants(self, address, abi):
    contract_instance = self.w3.eth.contract(address=address, abi=abi)
    try:
      name = contract_instance.functions.name().call()
    except:
      name = None
    try:
      symbol = contract_instance.functions.symbol().call()
    except:
      symbol = None
    return (name, symbol)
  def search_methods(self):
    standards = self._extract_methods_signatures()
    token_abi = self._get_standard_token_abi()
    non_standard_tokens = []
    # Iterate trough all contracts
    for contracts_chunk in self._iterate_contracts():
      for contract in contracts_chunk:
        # Download contract bytecode
        contract_checksum_addr = self.w3.toChecksumAddress(contract['_source']['address'])
        contract_code_bytearr = self.w3.eth.getCode(contract_checksum_addr)
        code = self.w3.toHex(contract_code_bytearr)
        # If contract is token - checks for standards
        is_token = re.search(r'' + standards['erc20']['transfer'], code) != None
        if is_token == True:
          # Check if contract bytecode complies with defined standards
          token_standards = self._check_standards(code, standards)
          if len(token_standards) > 0:
            name, symbol = self._search_constants(contract_checksum_addr, token_abi)
            update_body = {
              'standards': token_standards, 
              'bytecode': code, 
              'token_name': name, 
              'token_symbol': symbol, 
              'is_token': True
            }
            self.client.update(self.index, 'contract', contract['_id'], doc=update_body)
          else:
            non_standard_tokens.append({'address': contract_checksum_addr, 'bytecode': code, 'id': contract['_id']})
        else:
          self.client.update(self.index, 'contract', contract['_id'], doc={'is_token': False, 'bytecode': code})
    for token in non_standard_tokens:
      name, symbol = self._search_constants(token['address'], token_abi)
      update_body = {'standards': None, 'bytecode': token['bytecode'], 'token_name': name, 'token_symbol': symbol, 'is_token': True}
      self.client.update(self.index, 'contract', token['id'], doc=update_body)
        
