import os
import json
from multiprocessing import Pool
from config import PARITY_HOSTS, INDICES, ETHERSCAN_API_KEY, INPUT_PARSING_PROCESSES
import utils
from clients.custom_clickhouse import CustomClickhouse
import requests

ETHERSCAN_ABI_API = "https://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey=" + ETHERSCAN_API_KEY
NUMBER_OF_PROCESSES = INPUT_PARSING_PROCESSES

def _get_contracts_abi_sync(addresses):
    """
    Get ABIs for specified list of addresses

    Parameters
    ----------
    addresses : list
        List of contract addresses

    Returns
    -------
    dict
        ABIs for specified addresses. Each ABI is a list.
        Each list can be empty when there is a problem with ABI extraction for this address
    """
    abis = {}
    for key, address in addresses.items():
        api_url = ETHERSCAN_ABI_API.format(address)
        abi = requests.get(api_url).json()
        try:
            abis[key] = json.loads(abi["result"])
        except:
            abis[key] = []
    return abis


class ClickhouseContracts(utils.ClickhouseContractTransactionsIterator):
    doc_type = "itx"
    index = "internal_transaction"
    block_prefix = "abi_extracted"

    def __init__(self, indices=INDICES, parity_hosts=PARITY_HOSTS):
        self.indices = indices
        self.client = CustomClickhouse()
        self.pool = Pool(processes=NUMBER_OF_PROCESSES)
        self.parity_hosts = parity_hosts

    def _split_on_chunks(self, iterable, size):
        """
        Split given iterable onto chunks
        """
        return utils.split_on_chunks(iterable, size)

    def _get_contracts_abi(self, all_addresses):
        """
        Get ABI for specified contracts in parallel mode

        Parameters
        ----------
        all_addresses : list
            Contract addresses
        Returns
        -------
        list
            List of ABIs for each contract in list
        """
        chunks = self._split_on_chunks(list(enumerate(all_addresses)), NUMBER_OF_PROCESSES)
        dict_chunks = [dict(chunk) for chunk in chunks]
        abis = {key: abi for abis_dict in self.pool.map(_get_contracts_abi_sync, dict_chunks) for key, abi in
                abis_dict.items()}
        return [abis[key] for key in sorted(abis.keys())]

    def _get_range_query(self):
        """
        Get range query based on all specified blocks range in config.py

        Returns
        -------
        str
            SQL query for blockNumber located in specified range
        """
        ranges = [range_tuple[0:2] for range_tuple in self.parity_hosts]
        range_query = utils.make_range_query("blockNumber", *ranges)
        return range_query

    def _iterate_contracts_without_abi(self):
        """
        Iterate through contracts without previous attemp to extract ABI from etherscan.io
        within block range specified in config.py.

        Returns
        -------
        generator
            Generator that iterates through contracts by conditions above
        """
        query = 'ANY LEFT JOIN {} USING id WHERE abi_extracted IS NULL AND {}'.format(
            self.indices["contract_abi"],
            self._get_range_query()
        )
        return self._iterate_contracts(partial_query=query, fields=["address"])

    def _convert_abi(self, abi):
        """
        Return JSON string for given ABI if it is not empty. Otherwise return None
        """
        if abi:
            return json.dumps(abi)
        else:
            return None

    def save_contracts_abi(self):
        """
        Save contracts ABI to a database

        This function is an entry point for download-contracts-abi operation
        """
        for contracts in self._iterate_contracts_without_abi():
            abis = self._get_contracts_abi([contract["_source"]["address"] for contract in contracts])
            documents = [{'abi': self._convert_abi(abis[index]), 'abi_extracted': True, "id": contract["_id"]} for
                         index, contract in enumerate(contracts)]
            self.client.bulk_index(index=self.indices["contract_abi"], docs=documents)
