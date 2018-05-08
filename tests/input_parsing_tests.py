from contracts import Contracts
import os
import unittest
import subprocess
from time import sleep
from test_utils import TestElasticSearch
from tqdm import *
import json

class InputParsingTestCase(unittest.TestCase):
  def setUp(self):
    self.contracts = Contracts(TEST_INDEX)
    self.client = TestElasticSearch()
    self.client.recreate_fast_index(TEST_INDEX)

  def test_set_contracts_abi(self):
    self.contracts._set_contracts_abi([TEST_CONTRACT_ABI, TEST_CONTRACT_ABI])
    self.assertCountEqual(self.contracts._contracts_abi, TEST_CONTRACT_ABI + TEST_CONTRACT_ABI)

  def test_get_contract_abi(self):
    response = self.contracts._get_contract_abi(TEST_CONTRACT_ADDRESS)
    self.assertSequenceEqual(response, TEST_CONTRACT_ABI)

  def test_get_wrong_contract_abi(self):
    response = self.contracts._get_contract_abi("0x0")
    assert response == []

  def test_get_uncached_contract_abi(self):
    try:
      os.remove("/home/anatoli/.quickBlocks/cache/abis/" + TEST_CONTRACT_ADDRESS + ".json")
    except:
      pass
    response = self.contracts._get_contract_abi(TEST_CONTRACT_ADDRESS)
    self.assertSequenceEqual(response, TEST_CONTRACT_ABI)

  def test_decode_inputs_batch(self):
    self.contracts._set_contracts_abi([TEST_CONTRACT_ABI])
    response = self.contracts._decode_inputs_batch([TEST_CONTRACT_PARAMETERS, TEST_CONTRACT_PARAMETERS])
    self.assertSequenceEqual(response, [TEST_CONTRACT_DECODED_PARAMETERS, TEST_CONTRACT_DECODED_PARAMETERS])

  def add_contracts_with_and_without_abi(self):
    for i in tqdm(range(10)):
      self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS}, id=i + 1, refresh=True)
    for i in tqdm(range(10)):
      self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS, 'abi': True}, id=i + 11, refresh=True)

  def test_iterate_contracts_without_abi(self):
    self.add_contracts_with_and_without_abi()
    contracts = [c for c in self.contracts._iterate_contracts_without_abi()]
    contracts = [c["_id"] for contracts_list in contracts for c in contracts_list]
    self.assertCountEqual(contracts, [str(i) for i in range(1, 11)])

  def test_save_contracts_abi(self):
    for i in tqdm(range(10)):
      self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS}, id=i + 1, refresh=True)
    self.contracts._save_contracts_abi()
    contracts = self.client.search(index=TEST_INDEX, doc_type='contract', query="abi:*", size=100)['hits']['hits']
    abis = [contract["_source"]["abi"] for contract in contracts]
    self.assertCountEqual(abis, [TEST_CONTRACT_ABI] * 10)

  def test_iterate_contracts_with_abi(self):
    self.add_contracts_with_and_without_abi()
    contracts = [c for c in self.contracts._iterate_contracts_with_abi()]
    contracts = [c["_id"] for contracts_list in contracts for c in contracts_list]
    self.assertCountEqual(contracts, [str(i) for i in range(11, 21)])

  def test_iterate_transactions_by_targets(self):
    for i in tqdm(range(20)):
      self.client.index(TEST_INDEX, 'tx', {'to': TEST_CONTRACT_ADDRESS}, id=i + 1, refresh=True)
    for i in tqdm(range(20)):
      self.client.index(TEST_INDEX, 'tx', {'to': "0x"}, id=i + 21, refresh=True)
    targets = [TEST_CONTRACT_ADDRESS]
    transactions = [c for c in self.contracts._iterate_transactions_by_targets(targets)]
    transactions = [t["_id"] for transactions_list in transactions for t in transactions_list]
    self.assertCountEqual(transactions, [str(i) for i in range(1, 21)])    

  def test_decode_inputs_for_contracts(self):
    self.contracts._set_contracts_abi([TEST_CONTRACT_ABI])
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS, 'abi': TEST_CONTRACT_ABI}, id=1, refresh=True)
    for i in tqdm(range(10)):
      self.client.index(TEST_INDEX, 'tx', {'to': TEST_CONTRACT_ADDRESS, 'input': TEST_CONTRACT_PARAMETERS}, id=i + 1, refresh=True)
    contracts = self.client.search(index=TEST_INDEX, doc_type='contract', query="abi:*")['hits']['hits']
    self.contracts._decode_inputs_for_contracts(contracts)
    transactions = self.client.search(index=TEST_INDEX, doc_type='tx', query="*")['hits']['hits']
    decoded_inputs = [t["_source"]["decoded_input"] for t in transactions]
    self.assertCountEqual(decoded_inputs, [TEST_CONTRACT_DECODED_PARAMETERS] * 10)

  def test_decode_inputs_for_big_portion_of_contracts(self):
    for i in tqdm(range(10)):
      self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS}, id=i + 1, refresh=True)
    for i in tqdm(range(10)):
      self.client.index(TEST_INDEX, 'tx', {'to': TEST_CONTRACT_ADDRESS, 'input': TEST_CONTRACT_PARAMETERS}, id=i + 1, refresh=True)
    self.contracts.decode_inputs()
    transactions = self.client.search(index=TEST_INDEX, doc_type='tx', query="*")['hits']['hits']
    assert len(transactions) == 10

TEST_CONTRACT_ABI = json.loads('[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"bytes32"}],"payable":false,"type":"function"},{"constant":false,"inputs":[],"name":"stop","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"owner_","type":"address"}],"name":"setOwner","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint128"}],"name":"push","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"name_","type":"bytes32"}],"name":"setName","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint128"}],"name":"mint","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"src","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"stopped","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"authority_","type":"address"}],"name":"setAuthority","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"wad","type":"uint128"}],"name":"pull","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint128"}],"name":"burn","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"bytes32"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[],"name":"start","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"authority","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"src","type":"address"},{"name":"guy","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"inputs":[{"name":"symbol_","type":"bytes32"}],"payable":false,"type":"constructor"},{"anonymous":true,"inputs":[{"indexed":true,"name":"sig","type":"bytes4"},{"indexed":true,"name":"guy","type":"address"},{"indexed":true,"name":"foo","type":"bytes32"},{"indexed":true,"name":"bar","type":"bytes32"},{"indexed":false,"name":"wad","type":"uint256"},{"indexed":false,"name":"fax","type":"bytes"}],"name":"LogNote","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"authority","type":"address"}],"name":"LogSetAuthority","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"}],"name":"LogSetOwner","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"}]')
TEST_CONTRACT_ADDRESS = '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0'
TEST_CONTRACT_PARAMETERS = '0xa9059cbb000000000000000000000000d11b80088ce2623a9c017b93008405511cd951d200000000000000000000000000000000000000000000000d343b16da9c1a4000'
TEST_CONTRACT_DECODED_PARAMETERS = {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xd11b80088ce2623a9c017b93008405511cd951d2'}, {'type': 'uint256', 'value': '243571300000000000000'}]}
TEST_INDEX = 'test-ethereum-transactions'