import api.server as server
from api.server import get_holders_number, get_incomes, get_outcomes, get_balances
import unittest
from test_utils import TestElasticSearch
from unittest.mock import MagicMock, Mock, patch, call

class APITestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_CONTRACTS_INDEX)
    self.client.recreate_index(TEST_TOKEN_TRANSACTIONS_INDEX)
    server.app.config.update({
      "token_tx": TEST_TOKEN_TRANSACTIONS_INDEX,
      "contract": TEST_CONTRACTS_INDEX
    })
    self.app = server.app.test_client()

  def test_get_token_holders_number(self):
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

    assert 2 == get_holders_number("0x1")

  def test_get_incomes(self):
    test_incomes = {
      "0x1": 10000,
      "0x2": 10000
    }
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      "to": "0x1",
      "from": "0x2",
      "token": "0x1",
      "value": "10000"
    }, refresh=True)
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      "to": "0x2",
      "from": "0x1",
      "token": "0x1",
      "value": "10000"
    }, refresh=True)
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      "to": "0x2",
      "from": "0x1",
      "token": "0x0",
      "value": "10000"
    }, refresh=True)

    received_incomes = get_incomes("0x1")
    self.assertSequenceEqual(test_incomes, received_incomes)

  def test_get_incomes_for_many_token_holders(self):
    test_incomes = {"0x" + str(i): 1 for i in range(10000)}
    docs = [{
      "to": "0x" + str(i),
      "from": "0x2",
      "value": "1",
      "token": "0x1"
    } for i in range(10000)]
    self.client.bulk_index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", docs=docs, refresh=True)

    received_incomes = get_incomes("0x1")
    self.assertSequenceEqual(test_incomes, received_incomes)

  def test_get_outcomes(self):
    test_outcomes = {
      "0x1": 10000,
      "0x2": 10000
    }
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      "from": "0x1",
      "to": "0x2",
      "token": "0x1",
      "value": "10000"
    }, refresh=True)
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      "from": "0x2",
      "to": "0x1",
      "token": "0x1",
      "value": "10000"
    }, refresh=True)
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      "from": "0x2",
      "to": "0x1",
      "token": "0x0",
      "value": "10000"
    }, refresh=True)

    received_outcomes = get_outcomes("0x1")
    self.assertSequenceEqual(test_outcomes, received_outcomes)

  def test_get_outcomes_for_many_token_holders(self):
    test_outcomes = {"0x" + str(i): 1 for i in range(10000)}
    docs = [{
      "from": "0x" + str(i),
      "to": "0x2",
      "value": "1",
      "token": "0x1"
    } for i in range(10000)]
    self.client.bulk_index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", docs=docs, refresh=True)

    received_outcomes = get_outcomes("0x1")
    self.assertSequenceEqual(test_outcomes, received_outcomes)

  def test_get_balances(self):
    test_outcomes = {
      "0x1": 1,
      "0x2": 1
    }
    test_incomes = {
      "0x1": 1,
      "0x3": 1
    }
    test_balances = {
      "0x1": 0,
      "0x2": -1,
      "0x3": 1
    }
    test_incomes_mock = MagicMock(return_value=test_incomes)
    test_outcomes_mock = MagicMock(return_value=test_outcomes)
    with patch('api.server.get_incomes', test_incomes_mock), \
         patch('api.server.get_outcomes', test_outcomes_mock):
      process = Mock(
        incomes=test_incomes_mock,
        outcomes=test_outcomes_mock
      )

      received_balances = get_balances("0x1")

      process.assert_has_calls([
        call.incomes("0x1"),
        call.outcomes("0x1")
      ])
      self.assertSequenceEqual(test_balances, received_balances)

  def test_get_balances_api(self):
    test_balances = {
      "0x1": 1,
      "0x2": 2,
      "0x3": 3
    }
    test_balances_mock = MagicMock(return_value=test_balances)
    with patch("api.server.get_balances", test_balances_mock):
      received_balances = self.app.get("/balances?token=0x0").json
      test_balances_mock.assert_called_with("0x0")
      self.assertSequenceEqual(test_balances, received_balances)

TEST_CONTRACTS_INDEX = 'test-ethereum-contracts'
TEST_TOKEN_TRANSACTIONS_INDEX = 'test-ethereum-transactions'