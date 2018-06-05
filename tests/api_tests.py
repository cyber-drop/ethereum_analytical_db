import api.server as server
import unittest
from test_utils import TestElasticSearch

class APITestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_CONTRACTS_INDEX)
    self.client.recreate_index(TEST_TOKEN_TRANSACTIONS_INDEX)
    self.app = server.app.test_client()

  def test_get_token_holders(self):
    test_token_holders = ["0x1", "0x2"]
    self.client.index(
      index=TEST_TOKEN_TRANSACTIONS_INDEX,
      doc_type="tx",
      doc={"to": "0x1", "from": "0x2", "token": "0x1"},
      refresh=True
    )

    received_token_holders = self.app.get("holders?token=0x1").json

    self.assertCountEqual(test_token_holders, received_token_holders)

  def test_get_token_holders_for_different_token(self):
    test_token_holders = ["0x1", "0x3"]
    self.client.index(
      index=TEST_TOKEN_TRANSACTIONS_INDEX,
      doc_type="tx",
      doc={"to": "0x1", "from": "0x2", "token": "0x0"},
      refresh=True
    )
    self.client.index(
      index=TEST_TOKEN_TRANSACTIONS_INDEX,
      doc_type="tx",
      doc={"to": "0x1", "from": "0x3", "token": "0x1"},
      refresh=True
    )

    received_token_holders = self.app.get("holders?token=0x1").json

    self.assertCountEqual(test_token_holders, received_token_holders)

  def test_get_incomes(self):
    pass

TEST_CONTRACTS_INDEX = 'test-ethereum-contracts'
TEST_TOKEN_TRANSACTIONS_INDEX = 'test-ethereum-transactions'