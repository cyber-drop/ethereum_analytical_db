import re
from web3 import Web3, HTTPProvider
from custom_elastic_search import CustomElasticSearch
from config import INDICES, PARITY_HOSTS
import json
import math
from decimal import Decimal
from pyelasticsearch import bulk_chunks
import os
import utils

CURRENT_DIR = os.getcwd()

if "tests" in CURRENT_DIR:
  CURRENT_DIR = CURRENT_DIR[:-5]

with open('{}/standard-token-abi.json'.format(CURRENT_DIR)) as json_file:
  standard_token_abi = json.load(json_file)

class ContractMethods(utils.ContractTransactionsIterator):
  ''' 
  Check if contract is token, is it compliant with token standards and get variables from it such as name or symbol
  
  Parameters
  ----------
  elasticsearch_indices: dict
    Dictionary containing exisiting Elasticsearch indices
  elasticsearch_host: str
    Elasticsearch url
  parity_hosts: list
    List of tuples that includes 3 elements: start block, end_block, and Parity URL
  '''
  def __init__(self, elasticsearch_indices=INDICES, elasticsearch_host="http://localhost:9200", parity_hosts=PARITY_HOSTS):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.w3 = Web3(HTTPProvider(parity_hosts[0][2]))
    self.standard_token_abi = standard_token_abi
    self.standards = self._extract_methods_signatures()
    self.constants = ['name', 'symbol', 'decimals', 'total_supply', 'owner']

  def _iterate_unprocessed_contracts(self):
    '''
    Iterate over contracts that were not processed yet

    Returns
    -------
    generator
      Generator that iterates over contracts in Elasticsearch
    '''
    query = {
      "query_string": {
        "query": 'address:* AND !(methods:true)'
      }
    }
    return self._iterate_contracts(partial_query=query)

  def _extract_first_bytes(self, func):
    '''
    Create contract method signature and return first 4 bytes of this signature

    Parameters
    ----------
    func: str
      String that contains function name and arguments

    Returns
    -------
    str
      String with first 4 bytes of method signature in hex format
    '''
    return str(self.w3.toHex(self.w3.sha3(text=func)[0:4]))[2:]

  def _extract_methods_signatures(self):
    '''
    Return dictionary with first bytes of standard method signatures

    Returns
    -------
    dict
      Dictionary with first 4 bytes of methods signatures in hex format
    '''
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
    '''
    Check does contract bytecode contain first 4 bytes of transfer methods

    Parameters
    ----------
    bytecode: str
      String with contract bytecode

    Returns
    -------
    bool
      True if 4 bytes of signature is found; else False
    '''
    has_trasfer_method = re.search(r'' + self.standards['erc20']['transfer'], bytecode) != None
    return has_trasfer_method

  def _check_standards(self, bytecode):
    '''
    Check does contract bytecode contains all methods required to be compliant with token standard

    Parameters
    ----------
    bytecode: str
      String with contract bytecode

    Returns
    -------
    list
      List of token standards 
    '''
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
    '''
    Subtract decimals from contract total supply
    
    Return supply in string format to avoid Elasticsearch bigint problem
    Parameters
    ----------
    supply: int
      Contract total supply
    decimals: int
      Contract decimals 

    Returns
    -------
    str
      Contract total supply without decimals
    '''
    if decimals > 0:
      supply = supply / math.pow(10, decimals)
      supply = Decimal(supply)
      supply = round(supply)
      supply = str(supply)
    else: 
      supply = str(supply)
    return supply

  def _constant_methods(self, contract_instance):
    '''
    Return dict with methods used to extract values of contract public variables

    Parameters
    ----------
    contract_instance 
      An instance of Web3.eth.contract object

    Returns
    -------
    dict
      Dictionary whose keys are methods used to extract public variables
    '''
    return {
      'name': {'func': contract_instance.functions.name(), 'placeholder': None},
      'symbol': {'func': contract_instance.functions.symbol(), 'placeholder': None},
      'decimals': {'func': contract_instance.functions.decimals(),'placeholder': 18},
      'total_supply': {'func': contract_instance.functions.totalSupply(),'placeholder': '0'},
      'owner': {'func': contract_instance.functions.owner(), 'placeholder': None}
    }

  def _get_constants(self, address):
    '''
    Create an instance of a contract and get values of its public variables 
    
    Parameters
    ----------
    address: str
      Contract address

    Returns
    -------
    list
      List of values of available contract public variables
    '''
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
    '''
    Update contract document in Elasticsearch
    
    Parameters
    ----------
    doc_id: str
      id of Elasticsearch document
    body: dict
      Dictionary with new values
    '''
    self.client.update(self.indices['contract'], 'contract', doc_id, doc=body, refresh=True)

  def _classify_contract(self, contract):
    '''
    Check whether the contract is token, is it compliant with standards and if so, download its constants

    Parameters
    ----------
    contract: dict
      dictionary with contract address and bytecode
    '''
    code = contract['_source']['bytecode']
    is_token = self._check_is_token(code)
    if is_token == True:
      token_standards = self._check_standards(code)
      name, symbol, decimals, total_supply, owner = self._get_constants(contract['_source']['address'])
      update_body = {
        'standards': token_standards, 
        'token_name': name, 
        'token_symbol': symbol, 
        'decimals': decimals, 
        'total_supply': total_supply, 
        'token_owner': owner, 
        'is_token': True, 
        'methods': True
      }
      self._update_contract_descr(contract['_id'], update_body)
    else:
      update_body = {'is_token': False, 'methods': True}
      self._update_contract_descr(contract['_id'], update_body)

  def _construct_bulk_update_ops(self, docs):
    '''
    Return generator that creates document-updating operation used in bulk update

    Parameters
    ----------
    docs: list
      List of dictinoaries with new data
    '''
    for doc in docs:
      yield self.client.update_op(doc['doc'], id=doc['id'], upsert=doc['doc'])

  def _update_multiple_docs(self, docs, doc_type, index_name):
    ''' 
    Update multiple documents simultaneously

    Parameters
    ----------
    docs: list
      List of dictinoaries with new data
    doc_type: str
      Type of updated documents
    index_name: str
      Name of the index that comtains updated documents
    '''
    for chunk in bulk_chunks(self._construct_bulk_update_ops(docs), docs_per_chunk=1000):
      self.client.bulk(chunk, doc_type=doc_type, index=index_name, refresh=True)

  def _add_cmc_id(self):
    '''
    Add identificators used in Cryptocompare and Coinmarketcap to contract documents
    '''
    with open('{}/tokens.json'.format(CURRENT_DIR)) as json_file:
      tokens = json.load(json_file)
    update_docs = [{'doc': {
      'cmc_id': token['cmc_id'], 
      'website_slug': token['website_slug'],
      'cc_sym': token['cc_sym'],
      'address': token['address']
    }, 'id': token['address']} for token in tokens]
    self._update_multiple_docs(update_docs, 'contract', self.indices['contract'])

  def search_methods(self):
    ''' 
    Classify contracts into standard tokens, non-standard and non-tokens, than extract public variables values

    This function is an entry point for search-methods operation
    '''
    for contracts_chunk in self._iterate_unprocessed_contracts():
      for contract in contracts_chunk:
        self._classify_contract(contract)
    self._add_cmc_id()