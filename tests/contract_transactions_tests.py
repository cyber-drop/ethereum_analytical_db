import unittest 
from internal_transactions import ContractTransactions
from pyelasticsearch import ElasticSearch
from time import sleep

class ContractTransactionsTestCase(unittest.TestCase):
  def setUp(self):
    self.client = ElasticSearch('http://localhost:9200')
    try:
      self.client.delete_index(TEST_INDEX)
    except:
      pass
    self.client.create_index(TEST_INDEX)
    self.contract_transactions = ContractTransactions(TEST_INDEX)

  def test_extract_contract_addresses(self):
    self.client.index(TEST_INDEX, 'tx', {'input': '0x', 'to': TEST_TRANSACTION_TO}, id=1)
    self.client.index(TEST_INDEX, 'tx', {'input': TEST_TRANSACTION_INPUT, 'to': TEST_TRANSACTION_TO}, id=2)
    self.client.index(TEST_INDEX, 'tx', {'input': None, 'to': TEST_TRANSACTION_TO}, id=3)
    self.client.index(TEST_INDEX, 'nottx', {'input': TEST_TRANSACTION_INPUT, 'to': TEST_TRANSACTION_TO}, id=4)
    sleep(1)
    self.contract_transactions.extract_contract_addresses()
    sleep(1)
    contracts = self.client.search("address:*", index=TEST_INDEX, doc_type="contract")['hits']['hits']
    contracts = [contract['_source'] for contract in contracts]
    self.assertCountEqual([{'address': TEST_TRANSACTION_TO}], contracts)

  def extract_unique_contract_addresses(self):
    assert False

  def test_get_transactions_by_target(self):
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO}, id=1)
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO_CONTRACT}, id=2)
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO_COMMON}, id=3)
    sleep(1)
    transactions = self.contract_transactions._search_transactions_by_target([TEST_TRANSACTION_TO, TEST_TRANSACTION_TO_CONTRACT])
    transactions = [transaction['_id'] for transaction in transactions]
    self.assertCountEqual(["1", "2"], transactions)

  def test_detect_contract_transactions(self):
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO}, id=1)
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO_CONTRACT}, id=2)
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO_COMMON}, id=3)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TRANSACTION_TO}, id=1)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TRANSACTION_TO_CONTRACT}, id=2)
    sleep(1)
    self.contract_transactions.detect_contract_transactions()
    sleep(1)
    transactions = self.client.search("to_contract:true", index=TEST_INDEX, doc_type="tx")['hits']['hits']
    transactions = [transaction['_id'] for transaction in transactions]
    self.assertCountEqual(["1", "2"], transactions)

TEST_INDEX = 'test-ethereum-transactions'
TEST_TRANSACTION_INPUT = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_TO = '0xb1631db29e09ec5581a0ec398f1229abaf105d3524c49727621841af947bdc44'
TEST_TRANSACTION_TO_COMMON = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_TO_CONTRACT = '0x69a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'