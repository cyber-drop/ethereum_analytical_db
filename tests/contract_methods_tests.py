import unittest
from contract_methods import ContractMethods
from pyelasticsearch import ElasticSearch

class ContractMethodsTestCase(unittest.TestCase):
  def setUp(self):
    self.client = ElasticSearch('http://localhost:9200')
    try:
      self.client.delete_index(TEST_INDEX)
    except:
      pass
    self.client.create_index(TEST_INDEX)
    self.contract_methods = ContractMethods(TEST_INDEX)

  def test_iterate_contracts(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESSES[0]}, id=1, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESSES[1]}, id=2, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESSES[2]}, id=3, refresh=True)
    iterator = self.contract_methods._iterate_contracts()
    contracts = [c for contracts_list in iterator for c in contracts_list]
    contracts = [contract['_id'] for contract in contracts]
    self.assertCountEqual(["1", "2", "3"], contracts)
  
  def test_search_methods(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESSES[0]}, id=1, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESSES[1]}, id=2, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESSES[2]}, id=3, refresh=True)
    self.contract_methods.search_methods()
    iterator = self.contract_methods._iterate_contracts()
    contracts = [c for contracts_list in iterator for c in contracts_list]
    contracts = [contract['_source']['standards'] for contract in contracts]
    self.assertCountEqual([['erc20'], ['erc20'], ['erc20']], contracts)
 

TEST_INDEX = 'test-ethereum-contracts'
TEST_CONTRACT_ADDRESSES = ['0xa0e89120768bf166d228988627e4ac8af350220a', '0x6d6fb0951b769a6246f0246472856b2f70049c53', '0xaff9f95b455662c893bf3bb752557faa962d8355']
