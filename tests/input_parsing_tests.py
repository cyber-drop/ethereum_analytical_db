from contracts import Contracts
import os
import unittest
import subprocess
from time import sleep
from test_utils import TestElasticSearch
from tqdm import *

class InputParsingTestCase(unittest.TestCase):
  def setUp(self):
    self.contracts = Contracts(TEST_INDEX)
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_INDEX)

  def _run_shell_command(self, command):
    proc = subprocess.Popen(command,stdout=subprocess.PIPE,shell=True)
    (out, err) = proc.communicate()
    return out

  def test_restart_server_first_time(self):
    self.contracts._restart_server()
    processes = self._run_shell_command('lsof -i tcp:3000')
    assert len(processes) 

  def test_restart_server_again(self):
    self.contracts._restart_server()
    processes_before = self._run_shell_command('lsof -i tcp:3000')
    self.contracts._restart_server()
    processes_after = self._run_shell_command('lsof -i tcp:3000')
    assert processes_before != processes_after

  def test_restart_server_after_init(self):
    processes_before = self._run_shell_command('lsof -i tcp:3000')
    self.contracts = Contracts(TEST_INDEX)
    processes_after = self._run_shell_command('lsof -i tcp:3000')
    assert processes_before != processes_after

  def test_add_contract_abi(self):
    response = self.contracts._add_contract_abi(TEST_CONTRACT_ADDRESS)
    assert response['success'] == True
    assert os.path.isfile('/home/anatoli/.quickBlocks/cache/abis/' + TEST_CONTRACT_ADDRESS + ".json")

  def test_add_wrong_contract_abi(self):
    response = self.contracts._add_contract_abi("0x0")
    assert response['success'] == False

  def test_decode_input(self):
    self.contracts._add_contract_abi(TEST_CONTRACT_ADDRESS)
    response = self.contracts._decode_input(TEST_CONTRACT_PARAMETERS)
    self.assertSequenceEqual(response, TEST_CONTRACT_DECODED_PARAMETERS)

  def test_decode_input_for_wrong_contract_abi(self):
    self.contracts._add_contract_abi("0x0")
    response = self.contracts._decode_input(TEST_CONTRACT_PARAMETERS)
    self.assertSequenceEqual(response, {"contract_without_abi": True})

  def test_iterate_contracts(self):
    for i in tqdm(range(20)):
      self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS}, id=i + 1, refresh=True)
    contracts = [c for c in self.contracts._iterate_contracts()]
    contracts = [c["_id"] for contracts_list in contracts for c in contracts_list]
    self.assertCountEqual(contracts, [str(i) for i in range(1, 21)])

  def test_iterate_transactions_by_targets(self):
    for i in tqdm(range(20)):
      self.client.index(TEST_INDEX, 'tx', {'to': TEST_CONTRACT_ADDRESS}, id=i + 1, refresh=True)
    for i in tqdm(range(20)):
      self.client.index(TEST_INDEX, 'tx', {'to': "0x"}, id=i + 21, refresh=True)
    targets = [TEST_CONTRACT_ADDRESS]
    transactions = [c for c in self.contracts._iterate_transactions_by_targets(targets)]
    transactions = [t["_id"] for transactions_list in transactions for t in transactions_list]
    self.assertCountEqual(transactions, [str(i) for i in range(1, 21)])    

  def test_decode_inputs(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS}, id=1, refresh=True)
    for i in tqdm(range(10)):
      self.client.index(TEST_INDEX, 'tx', {'to': TEST_CONTRACT_ADDRESS, 'input': TEST_CONTRACT_PARAMETERS}, id=i + 1, refresh=True)
    contracts = [TEST_CONTRACT_ADDRESS]
    self.contracts._decode_inputs_for_contracts(contracts)
    transactions = self.client.search(index=TEST_INDEX, doc_type='tx', query="decoded_input:*")['hits']['hits']
    decoded_inputs = [t["_source"]["decoded_input"] for t in transactions]
    self.assertCountEqual(decoded_inputs, [TEST_CONTRACT_DECODED_PARAMETERS for i in range(1, 11)])

  def test_decode_inputs_for_big_portion_of_contracts(self):
    for i in tqdm(range(10)):
      self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS}, id=i + 1, refresh=True)
    for i in tqdm(range(10)):
      self.client.index(TEST_INDEX, 'tx', {'to': TEST_CONTRACT_ADDRESS, 'input': TEST_CONTRACT_PARAMETERS}, id=i + 1, refresh=True)
    self.contracts.decode_inputs()
    transactions = self.client.search(index=TEST_INDEX, doc_type='tx', query="decoded_input:*")['hits']['hits']
    assert len(transactions) == 10

TEST_CONTRACT_ADDRESS = '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0'
TEST_CONTRACT_PARAMETERS = '0xa9059cbb000000000000000000000000d11b80088ce2623a9c017b93008405511cd951d200000000000000000000000000000000000000000000000d343b16da9c1a4000'
TEST_CONTRACT_DECODED_PARAMETERS = {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xd11b80088ce2623a9c017b93008405511cd951d2', 'name': 'dst'}, {'type': 'uint256', 'value': '243571300000000000000', 'name': 'wad'}]}
TEST_INDEX = 'test-ethereum-transactions'