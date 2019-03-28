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


class ClickhouseContractMethods:
    """
    Check if contract is token, is it compliant with token standards and get variables from it such as name or symbol

    Parameters
    ----------
    indices: dict
        Dictionary containing exisiting database indices
    parity_hosts: list
        List of tuples that includes 3 elements: start block, end_block and Parity URL
    """
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
        self._set_external_links()

    def _set_external_links(self):
        """
        Sets website slug and cmc_id for this object
        """
        with open('{}/tokens.json'.format(CURRENT_DIR)) as json_file:
            tokens = json.load(json_file)
        for token in tokens:
            self._external_links[token["address"]] = {
                "website_slug": token["website_slug"],
                "cmc_id": token["cmc_id"],
            }

    def _iterate_unprocessed_contracts(self):
        """
        Iterate over ERC20 contracts that were not processed yet

        Returns
        -------
        generator
            Generator that iterates over contracts in database
        """
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

    def _round_supply(self, supply, decimals):
        """
        Divide supply by 10 ** decimals, and round it

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
        """
        if decimals > 0:
            supply = supply / math.pow(10, decimals)
            supply = Decimal(supply)
            supply = round(supply)

        return min(supply, MAX_TOTAL_SUPPLY)

    def _get_constant(self, address, constant, types, placeholder=None):
        """
        Get value through contract function marked as constant

        Tries every type from types dict and returns first value that are not empty
        If it fails, returns placeholder

        Parameters
        ----------
        address: str
            Contract address
        constant: str
            Name of constant
        types: dict
            Dict with all possible types and converter functions for target value
        placeholder
            Default value for target value

        Returns
        -------
            Value returned by a contract and converted with the function
            Placeholder, if there are no non-empty values
        """
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
        """
        Return contract ERC20 info

        Parameters
        ----------
        address: str
            Contract address

        Returns
        -------
        list
            Name, symbol, decimals, total supply, owner address
        """
        contract_constants = []
        for constant, types, placeholder in self._constants_types:
            response = self._get_constant(address, constant, types, placeholder)
            contract_constants.append(response)
        contract_constants[3] = self._round_supply(contract_constants[3], contract_constants[2])
        return contract_constants

    def _update_contract_descr(self, doc_id, body):
        """
        Store contract description in database

        Parameters
        ----------
        doc_id: str
          id of contract
        body: dict
          Dictionary with new values
        """
        body["id"] = doc_id
        self.client.bulk_index(self.indices['contract_description'], docs=[body])

    def _get_external_links(self, address):
        """
        Add Cryptocompare and Coinmarketcap info as a field of this object
        """
        external_links = self._external_links.get(address, {
            "website_slug": None,
            "cmc_id": None
        })
        return external_links.get("website_slug"), external_links.get("cmc_id")

    def _classify_contract(self, contract):
        """
        Extract contract ERC20 info and stores it into the database

        Extracts ERC20 token description from parity and from token.json file

        Parameters
        ----------
        contract: dict
            Dictionary with contract info
        """
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

    def search_methods(self):
        """
        Extract public values for ERC20 contracts

        This function is an entry point for extract-tokens operation
        """
        for contracts_chunk in self._iterate_unprocessed_contracts():
            for contract in contracts_chunk:
                self._classify_contract(contract)