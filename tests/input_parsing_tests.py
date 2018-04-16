from contracts import Contracts
import os
import unittest
import subprocess
from time import sleep
from test_utils import TestElasticSearch

class InputParsingTestCase(unittest.TestCase):
  def setUp(self):
    self.contracts = Contracts()
    self.client = TestElasticSearch()
    self.client.create_test_index(TEST_INDEX)

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
    self.contracts = Contracts()
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
    print(response)
    self.assertSequenceEqual(response, TEST_CONTRACT_DECODED_PARAMETERS)

  def test_iterate_contracts(self):
    pass
    # for i in range(20):
    #   self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS}, id=i + 1)
    # contracts = [c for c in self.contracts._iterate_contracts()]
    # contracts = [c["_id"] for contracts_list in contracts for c in contracts_list]

  def test_decode_inputs(self):
    pass

  def test_decode_inputs_for_big_portion_of_contracts(self):
    pass

TEST_CONTRACT_ADDRESS = '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0'
TEST_CONTRACT_PARAMETERS = '0xa9059cbb000000000000000000000000d11b80088ce2623a9c017b93008405511cd951d200000000000000000000000000000000000000000000000d343b16da9c1a4000'
TEST_CONTRACT_DECODED_PARAMETERS = {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xd11b80088ce2623a9c017b93008405511cd951d2', 'name': 'dst'}, {'type': 'uint256', 'value': '243571300000000000000', 'name': 'wad'}]}
TEST_INDEX = 'test-ethereum-transactions'