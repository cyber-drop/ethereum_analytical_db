import re
from web3 import Web3, HTTPProvider
from config import INDICES, PARITY_HOSTS
import json
import math
from decimal import Decimal
import os
import utils
from clients.custom_clickhouse import CustomClickhouse

CURRENT_DIR = os.getcwd()
MAX_TOTAL_SUPPLY = 1 << 63 - 1

if "tests" in CURRENT_DIR:
    CURRENT_DIR = CURRENT_DIR[:-5]

with open('{}/standard-token-abi.json'.format(CURRENT_DIR)) as json_file:
    standard_token_abi = json.load(json_file)


class ClickhouseContractMethods():
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
    _external_links = {}
    _constants_types = [
        ('name', {
            "string": lambda x: str(x).replace("\\x00", ""),
            "bytes32": lambda x: str(x).replace("\\x00", "")[2:-1].strip()
        }, ''),
        ('symbol', {
            "string": lambda x: str(x).replace("\\x00", ""),
            "bytes32": lambda x: str(x).replace("\\x00", "")[2:-1].strip()
        }, ''),
        ('decimals', {
            "uint8": None
        }, 18),
        ('totalSupply', {
            "uint256": None
        }, 0),
        ('owner', {
            "address": lambda x: x.lower()
        }, None)
    ]

    def __init__(self, indices=INDICES, parity_hosts=PARITY_HOSTS):
        self.indices = indices
        self.client = CustomClickhouse()
        self.w3 = Web3(HTTPProvider(parity_hosts[0][2]))
        self.standard_token_abi = standard_token_abi
        self.standards = self._extract_methods_signatures()
        self._set_external_links()

    def _set_external_links(self):
        with open('{}/tokens.json'.format(CURRENT_DIR)) as json_file:
            tokens = json.load(json_file)
        for token in tokens:
            self._external_links[token["address"]] = {
                "website_slug": token["website_slug"],
                "cmc_id": token["cmc_id"],
            }

    def _iterate_unprocessed_contracts(self):
        '''
        Iterate over contracts that were not processed yet

        Returns
        -------
        generator
          Generator that iterates over contracts in Elasticsearch
        '''
        return self.client.iterate(
            index=self.indices["contract"],
            fields=["address"],
            query="""
                WHERE standard_erc20 = 1
                AND id not in(
                    SELECT id
                    FROM {} 
                )
            """.format(self.indices["contract_description"])
        )


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

        return min(supply, MAX_TOTAL_SUPPLY)

    def _get_constant(self, address, constant, types, placeholder=None):
        contract_checksum_addr = self.w3.toChecksumAddress(address)
        contract_abi = [{
            "constant": True,
            "inputs": [],
            "name": constant,
            "outputs": [{
                "name": "",
                "type": None
            }],
            "payable": False,
            "type": "function"
        }]
        response = None
        for constant_type, convert in types.items():
            try:
                contract_abi[0]["outputs"][0]["type"] = constant_type
                contract_instance = self.w3.eth.contract(address=contract_checksum_addr, abi=contract_abi)
                response = getattr(contract_instance.functions, constant)().call()
                if convert:
                    response = convert(response)
                if response:
                    return response
            except Exception as e:
                pass
        if type(response) != int:
            return placeholder
        else:
            return response

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
        contract_constants = []
        for constant, types, placeholder in self._constants_types:
            response = self._get_constant(address, constant, types, placeholder)
            contract_constants.append(response)
        contract_constants[3] = self._round_supply(contract_constants[3], contract_constants[2])
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
        body["id"] = doc_id
        self.client.bulk_index(self.indices['contract_description'], docs=[body])

    def _classify_contract(self, contract):
        '''
        Check whether the contract is token, is it compliant with standards and if so, download its constants

        Parameters
        ----------
        contract: dict
          dictionary with contract address and bytecode
        '''
        name, symbol, decimals, total_supply, owner = self._get_constants(contract['_source']['address'])
        website_slug, cmc_id = self._get_external_links(contract["_source"]["address"])
        update_body = {
            'token_name': name,
            'token_symbol': symbol,
            'decimals': decimals,
            'total_supply': total_supply,
            'token_owner': owner,
            "website_slug": website_slug,
            "cmc_id": cmc_id
        }
        self._update_contract_descr(contract['_id'], update_body)

    def _get_external_links(self, address):
        '''
        Add identificators used in Cryptocompare and Coinmarketcap to contract documents
        '''
        external_links = self._external_links.get(address, {
            "website_slug": None,
            "cmc_id": None
        })
        return external_links.get("website_slug"), external_links.get("cmc_id")

    def search_methods(self):
        '''
        Classify contracts into standard tokens, non-standard and non-tokens, than extract public variables values

        This function is an entry point for search-methods operation
        '''
        for contracts_chunk in self._iterate_unprocessed_contracts():
            for contract in contracts_chunk:
                self._classify_contract(contract)
        # self._add_cmc_id()
