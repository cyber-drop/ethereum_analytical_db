import unittest 
from contract_transactions import ContractTransactions
from pyelasticsearch import ElasticSearch
from time import sleep
from tqdm import *
from test_utils import TestElasticSearch

class ContractTransactionsTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_INDEX)
    self.client.reduce_index_size(TEST_INDEX)
    self.contract_transactions = ContractTransactions(TEST_INDEX)

  def test_iterate_contract_transactions(self):
    self.client.index(TEST_INDEX, 'tx', {'input': '0x', 'to': TEST_TRANSACTION_TO}, id=1, refresh=True)
    self.client.index(TEST_INDEX, 'tx', {'input': TEST_TRANSACTION_INPUT, 'to': TEST_TRANSACTION_TO}, id=2, refresh=True)
    self.client.index(TEST_INDEX, 'tx', {'input': None, 'to': TEST_TRANSACTION_TO}, id=3, refresh=True)
    self.client.index(TEST_INDEX, 'nottx', {'input': TEST_TRANSACTION_INPUT, 'to': TEST_TRANSACTION_TO}, id=4, refresh=True)
    self.client.index(TEST_INDEX, 'tx', {'input': TEST_TRANSACTION_INPUT, 'to': TEST_TRANSACTION_TO, 'to_contract': True}, id=5, refresh=True)
    iterator = self.contract_transactions._iterate_contract_transactions()
    transactions = next(iterator)
    transactions = [transaction['_id'] for transaction in transactions]
    self.assertCountEqual(['2'], transactions)    

  def test_extract_contract_addresses(self):
    for i in range(11):
      self.client.index(TEST_INDEX, 'tx', {'input': TEST_TRANSACTION_INPUT, 'to': TEST_TRANSACTION_TO + str(i)}, id=i + 1, refresh=True)
    self.contract_transactions._extract_contract_addresses()
    contracts = self.client.search("address:*", index=TEST_INDEX, doc_type="contract", size=11)['hits']['hits']
    contracts = [contract['_source'] for contract in contracts]
    self.assertCountEqual([{'address': TEST_TRANSACTION_TO + str(i)} for i in range(11)], contracts)

  def test_extract_unique_contract_addresses(self):
    for i in range(11):
      self.client.index(TEST_INDEX, 'tx', {'input': TEST_TRANSACTION_INPUT, 'to': TEST_TRANSACTION_TO}, id=i + 1, refresh=True)
    self.contract_transactions._extract_contract_addresses()
    contracts = self.client.search("address:*", index=TEST_INDEX, doc_type="contract")['hits']['hits']
    contracts = [contract['_source'] for contract in contracts]
    self.assertCountEqual([{'address': TEST_TRANSACTION_TO}], contracts)

  def test_extract_big_amount_of_contract_addresses(self):
    for i in range(100):
      self.client.index(TEST_INDEX, 'tx', {'input': TEST_TRANSACTION_INPUT, 'to': TEST_TRANSACTION_TO + str(i)}, id=i, refresh=True)
    self.contract_transactions._extract_contract_addresses()
    contracts = self.client.search("address:*", index=TEST_INDEX, doc_type="contract", size=100)['hits']['hits']
    assert len(contracts) == 100

  def test_set_flag_for_processed_transactions(self):
    pass

  def test_iterate_contracts(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TRANSACTION_TO}, id=1, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TRANSACTION_TO_CONTRACT}, id=2, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TRANSACTION_TO_CONTRACT, 'transactions_detected': True}, id=3, refresh=True)
    iterator = self.contract_transactions._iterate_contracts()
    contracts = [c for contracts_list in iterator for c in contracts_list]
    contracts = [contract['_id'] for contract in contracts]
    self.assertCountEqual(["1", "2"], contracts)   

  def test_detect_contract_transactions(self):
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO, 'input': TEST_TRANSACTION_INPUT}, id=1, refresh=True)
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO_CONTRACT, 'input': TEST_TRANSACTION_INPUT}, id=2, refresh=True)
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO_COMMON, 'input': '0x'}, id=3, refresh=True)
    self.contract_transactions.detect_contract_transactions()
    transactions = self.client.search("to_contract:true", index=TEST_INDEX, doc_type="tx")['hits']['hits']
    transactions = [transaction['_id'] for transaction in transactions]
    self.assertCountEqual(["1", "2"], transactions)

  def test_detect_big_amount_of_contract_transactions(self):
    docs = [{'to': str((i % 10) + 1), 'input': TEST_TRANSACTION_INPUT, 'id': i + 1} for i in range(100)]
    self.client.bulk_index(docs=docs, doc_type='tx', index=TEST_INDEX, refresh=True)
    self.contract_transactions.detect_contract_transactions()
    transactions = self.client.search("to_contract:true", index=TEST_INDEX, doc_type="tx", size=100)['hits']['hits']
    assert len(transactions) == 100    

  def test_detect_transactions_by_big_portion_of_contracts(self):
    docs = [{'to': TEST_TRANSACTION_TO + str(i), 'input': TEST_TRANSACTION_INPUT, 'id': i + 1} for i in range(1000)]
    self.client.bulk_index(docs=docs, doc_type='tx', index=TEST_INDEX, refresh=True)
    self.contract_transactions._detect_transactions_by_contracts([TEST_TRANSACTION_TO + str(i) for i in range(1000)])
    transactions = self.client.search("to_contract:true", index=TEST_INDEX, doc_type="tx", size=1000)['hits']['hits']
    assert len(transactions) == 1000

  def test_set_flag_for_processed_contracts(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TRANSACTION_TO}, id=1, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TRANSACTION_TO_CONTRACT}, id=2, refresh=True)
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO, 'input': TEST_TRANSACTION_INPUT}, id=1, refresh=True)
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO, 'input': TEST_TRANSACTION_INPUT}, id=2, refresh=True)
    self.client.index(TEST_INDEX, 'tx', {'to': TEST_TRANSACTION_TO, 'input': TEST_TRANSACTION_INPUT}, id=3, refresh=True)
    self.contract_transactions._detect_transactions_by_contracts([TEST_TRANSACTION_TO])
    contract = self.client.get(index=TEST_INDEX, doc_type="contract", id=1)["_source"]
    assert contract["transactions_detected"]

TEST_INDEX = 'test-ethereum-transactions'
TEST_TRANSACTION_INPUT = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_TO = '0xb1631db29e09ec5581a0ec398f1229abaf105d3524c49727621841af947bdc44'
TEST_TRANSACTION_TO_COMMON = '0x38a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'
TEST_TRANSACTION_TO_CONTRACT = '0x69a999ebba98a14a67ea7a83921e3e58d04a29fc55adfa124a985771f323052a'