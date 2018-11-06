from operations.contracts import _get_contracts_abi_sync, ClickhouseContracts
from operations import contracts
import os
import unittest
from tests.test_utils import TestElasticSearch, mockify, TestClickhouse
from tqdm import *
from unittest.mock import MagicMock, call, Mock, patch, ANY
import multiprocessing
import json
from operations.indices import ClickhouseIndices

TEST_CONTRACT_ABI = json.loads('[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"bytes32"}],"payable":false,"type":"function"},{"constant":false,"inputs":[],"name":"stop","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"owner_","type":"address"}],"name":"setOwner","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint128"}],"name":"push","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"name_","type":"bytes32"}],"name":"setName","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint128"}],"name":"mint","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"src","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"stopped","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"authority_","type":"address"}],"name":"setAuthority","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"wad","type":"uint128"}],"name":"pull","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint128"}],"name":"burn","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"bytes32"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[],"name":"start","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"authority","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"src","type":"address"},{"name":"guy","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"inputs":[{"name":"symbol_","type":"bytes32"}],"payable":false,"type":"constructor"},{"anonymous":true,"inputs":[{"indexed":true,"name":"sig","type":"bytes4"},{"indexed":true,"name":"guy","type":"address"},{"indexed":true,"name":"foo","type":"bytes32"},{"indexed":true,"name":"bar","type":"bytes32"},{"indexed":false,"name":"wad","type":"uint256"},{"indexed":false,"name":"fax","type":"bytes"}],"name":"LogNote","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"authority","type":"address"}],"name":"LogSetAuthority","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"}],"name":"LogSetOwner","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"}]')
TEST_CONTRACT_ADDRESS = '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0'
TEST_CONTRACT_PARAMETERS = '0xa9059cbb000000000000000000000000d11b80088ce2623a9c017b93008405511cd951d200000000000000000000000000000000000000000000000d343b16da9c1a4000'
TEST_CONTRACT_DECODED_PARAMETERS = {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xd11b80088ce2623a9c017b93008405511cd951d2'}, {'type': 'uint256', 'value': '243571300000000000000'}]}
TEST_TRANSACTIONS_INDEX = 'test_ethereum_transactions'
TEST_CONTRACTS_INDEX = 'test_ethereum_contracts'
TEST_CONTRACTS_ABI_INDEX = 'test_ethereum_contracts_abi'
TEST_CONTRACTS_BLOCK_INDEX = 'test_contract_block'

class ClickhouseContractABITestCase(unittest.TestCase):
  doc_type = "itx"
  index = "internal_transaction"
  doc = {'to': TEST_CONTRACT_ADDRESS, 'input': TEST_CONTRACT_PARAMETERS, "callType": "call", 'blockNumber': 10}
  blocks_query = "traces_extracted:true"
  contracts_class = ClickhouseContracts

  def setUp(self):
    self.client = TestClickhouse()
    self.indices = {
      "contract": TEST_CONTRACTS_INDEX,
      self.index: TEST_TRANSACTIONS_INDEX,
      "contract_abi": TEST_CONTRACTS_ABI_INDEX,
      'contract_block': TEST_CONTRACTS_BLOCK_INDEX
    }
    self.contracts = self.contracts_class(
      self.indices,
      parity_hosts=[(None, None, "http://localhost:8545")]
    )
    self.client.prepare_indices(self.indices)

  def test_pool(self):
    """Test pool size"""
    assert self.contracts.pool._processes == 10

  def test_get_contract_abi(self):
    """Test getting contract ABI by address"""
    response = _get_contracts_abi_sync({1: TEST_CONTRACT_ADDRESS})
    self.assertSequenceEqual(response, {1: TEST_CONTRACT_ABI})

  def test_get_wrong_contract_abi(self):
    """Test getting contract ABI by invalid address"""
    response = _get_contracts_abi_sync({"wrong": "0x0"})
    self.assertSequenceEqual(response, {"wrong": []})

  def test_get_uncached_contract_abi(self):
    """Test getting contract ABI with no record in cache"""
    try:
      os.remove("/home/noomkcalb/.quickBlocks/cache/abis/" + TEST_CONTRACT_ADDRESS + ".json")
    except:
      pass
    response = _get_contracts_abi_sync({"uncached": TEST_CONTRACT_ADDRESS})
    self.assertSequenceEqual(response, {"uncached": TEST_CONTRACT_ABI})

  def test_get_multiple_contracts_abi(self):
    """Test getting ABI for multiple contracts"""
    response = _get_contracts_abi_sync({1: TEST_CONTRACT_ADDRESS, 2: TEST_CONTRACT_ADDRESS})
    self.assertSequenceEqual(response, {1: TEST_CONTRACT_ABI, 2: TEST_CONTRACT_ABI})

  def test_split_on_chunks(self):
    """Test splitting on chunks"""
    test_list = list(range(10))
    test_chunks = list(self.contracts._split_on_chunks(test_list, 3))
    self.assertSequenceEqual(test_chunks, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])

  def test_get_contracts_abi(self):
    """Test getting ABI in parallel mode with right order"""
    addresses = ["address" + str(i) for i in range(100)]
    chunks = [[(0, "address1")], [(1, "address2")]]
    abis = [{1: "abi2"}, {0: "abi1"}]

    self.contracts._split_on_chunks = MagicMock(return_value=chunks)
    self.contracts.pool.map = MagicMock(return_value=abis)

    response = self.contracts._get_contracts_abi(addresses)

    self.contracts._split_on_chunks.assert_called_with([(index, address) for index, address in enumerate(addresses)], 10)
    self.contracts.pool.map.assert_called_with(contracts._get_contracts_abi_sync, [dict(chunk) for chunk in chunks])
    self.assertSequenceEqual(["abi1", "abi2"], response)

  def test_iterate_contracts_without_abi(self):
    """Test iterations through contracts without abi_extracted flag"""
    self.contracts = self.contracts_class(
      self.indices,
      parity_hosts=[(0, 8, "http://localhost:8545")]
    )
    self.contracts._get_max_block = MagicMock(return_value=2)
    self.add_contracts_with_and_without_abi()
    contracts = [c for c in self.contracts._iterate_contracts_without_abi()]
    contracts = [c["_id"] for contracts_list in contracts for c in contracts_list]
    self.assertCountEqual(contracts, [str(i) for i in range(1, 9)])

  def test_iterate_contracts_without_abi_call_iterate_contracts(self):
    """Test iterations through all contracts using limitations by whitelist"""
    test_iterator = "iterator"
    self.contracts._iterate_contracts = MagicMock(return_value=test_iterator)

    contracts = self.contracts._iterate_contracts_without_abi()

    self.contracts._iterate_contracts.assert_any_call(partial_query=ANY, fields=["address"])
    assert contracts == test_iterator

  def add_contracts_with_and_without_abi(self):
    """Add 10 contracts with no ABI at all, 10 contracts with abi_extracted flag and 5 contracts with ABI"""
    contracts = [{'address': TEST_CONTRACT_ADDRESS, "blockNumber": i, "id": i + 1} for i in range(25)]
    contracts_abi = [{'abi_extracted': True, 'id': i} for i in range(11, 21)]
    contracts_abi += [{'abi_extracted': True, 'abi': {"test": 1}, 'id': i + 11} for i in range(21, 26)]
    self.client.bulk_index(TEST_CONTRACTS_INDEX, contracts)
    self.client.bulk_index(TEST_CONTRACTS_ABI_INDEX, contracts_abi)

  def test_save_contracts_abi(self):
    """Test saving ABI for each contract in Clickhouse"""
    test_contracts = [{"blockNumber": i, 'address': TEST_CONTRACT_ADDRESS, 'id': i + 1} for i in range(10)]
    self.client.bulk_index(TEST_CONTRACTS_INDEX, test_contracts)
    self.contracts.save_contracts_abi()
    contracts = self.client.search(index=TEST_CONTRACTS_ABI_INDEX, query="WHERE abi IS NOT NULL", fields=["abi"])
    abis = [json.loads(contract["_source"]["abi"]) for contract in contracts]
    self.assertCountEqual(abis, [TEST_CONTRACT_ABI] * 10)

  def test_save_contracts_abi_status(self):
    """Test saving abi_extracted flag for each contract in Clickhouse"""
    test_contracts = [{"blockNumber": i, 'address': TEST_CONTRACT_ADDRESS, 'id': i + 1} for i in range(10)]
    self.client.bulk_index(TEST_CONTRACTS_INDEX, test_contracts)
    self.contracts.save_contracts_abi()
    contracts_count = self.client.count(index=TEST_CONTRACTS_ABI_INDEX, query="WHERE abi_extracted = 1")
    assert contracts_count == 10