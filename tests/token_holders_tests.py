import unittest
from token_holders import TokenHolders
from contract_methods import ContractMethods
from test_utils import TestElasticSearch
import json

class TokenHoldersTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_INDEX)
    self.token_holders = TokenHolders({"contract": TEST_INDEX})
    self.contract_methods = ContractMethods({"contract": TEST_INDEX})

  def test_search_multiple_tokens(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[0]}, id=1, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[1]}, id=2, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[2]}, id=3, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[3]}, id=4, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[4]}, id=5, refresh=True)
    self.contract_methods.search_methods()
    names = ['Aeternity', 'Populous Platform', 'Golem Network Token']
    tokens = self.token_holders._search_multiple_tokens(names)
    results_count = len(tokens)
    tokens = [token[0]['_source']['token_name'] for token in tokens]
    self.assertCountEqual(['Aeternity', 'Populous Platform', 'Golem Network Token'], tokens)

  def test_search_duplicates(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[0]}, id=1, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[1]}, id=2, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[2]}, id=3, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[3]}, id=4, refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[4]}, id=5, refresh=True)
    self.contract_methods.search_methods()
    duplicated = self.token_holders._search_duplicates()
    duplicates_count = len(duplicated)
    duplicated = [token[0]['_source']['token_name'] for token in duplicated]
    assert 'Aeternity' in duplicated
    assert duplicates_count == 2

TEST_INDEX = 'test-ethereum-contracts'
TEST_TOKEN_ADDRESSES = ['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d',
  '0xd4fa1460f537bb9085d22c7bccb5dd450ef28e3a',
  '0x51ada638582e51c931147c9abd2a6d63bc02e337',
  '0xa74476443119a942de498590fe1f2454d7d4ac0d',
  '0xbe78d802c2aeebdc34c810b805c2691885a61257']
    