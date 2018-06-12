import api.server as server
from api.server import get_holders_number, get_token_incomes, \
  get_token_outcomes, get_token_balances
import unittest
from tests.test_utils import TestElasticSearch
from unittest.mock import MagicMock, Mock, patch, call

class InOutTransactionsTestCase():
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_CONTRACTS_INDEX)
    self.client.recreate_index(TEST_TOKEN_TRANSACTIONS_INDEX)
    server.app.config.update({
      "token_tx": TEST_TOKEN_TRANSACTIONS_INDEX,
      "contract": TEST_CONTRACTS_INDEX
    })

  def test_get_state(self):
    test_incomes = {
      "0x1": 10000,
      "0x2": 10000
    }
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      self.to_field: "0x1",
      self.from_field: "0x2",
      "token": "0x1",
      "value": 10000,
    }, refresh=True)
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      self.to_field: "0x2",
      self.from_field: "0x1",
      "token": "0x1",
      "value": 10000,
    }, refresh=True)
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      self.to_field: "0x2",
      self.from_field: "0x1",
      "token": "0x0",
      "value": 10000,
    }, refresh=True)

    received_incomes = self._call_method("0x1")
    self.assertSequenceEqual(test_incomes, received_incomes)

  def test_get_state_with_none_value(self):
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      self.to_field: "0x2",
      self.from_field: "0x1",
      "token": "0x1",
      "value": None,
    }, refresh=True)

    received_incomes = self._call_method("0x1")
    self.assertSequenceEqual({}, received_incomes)

  def test_get_state_for_approve_transactions(self):
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      self.to_field: "0x2",
      self.from_field: "0x1",
      "token": "0x1",
      "value": 10,
      "method": "approve"
    }, refresh=True)

    received_incomes = self._call_method("0x1")
    self.assertSequenceEqual({}, received_incomes)

  def test_get_state_with_invalid_transactions(self):
    docs = [{
      self.to_field: "0x1",
      self.from_field: "0x2",
      "valid": False,
      "value": 100,
      "token": "0x1"
    } for i in range(10000)]
    self.client.bulk_index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", docs=docs, refresh=True)

    received_incomes = self._call_method("0x1")
    self.assertSequenceEqual({}, received_incomes)

  def test_get_state_for_many_token_holders(self):
    test_incomes = {"0x" + str(i): 1 for i in range(10000)}
    docs = [{
      self.to_field: "0x" + str(i),
      self.from_field: "0x2",
      "value": 1,
      "token": "0x1",
    } for i in range(10000)]
    self.client.bulk_index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", docs=docs, refresh=True)

    received_incomes = self._call_method("0x1")
    self.assertSequenceEqual(test_incomes, received_incomes)

  def test_get_state_before_block(self):
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      self.to_field: "0x2",
      self.from_field: "0x1",
      "token": "0x1",
      "value": 1,
      "block_id": 0,
    }, refresh=True)
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      self.to_field: "0x2",
      self.from_field: "0x1",
      "token": "0x1",
      "value": 1,
      "block_id": 1,
    }, refresh=True)
    self.client.index(index=TEST_TOKEN_TRANSACTIONS_INDEX, doc_type="tx", doc={
      self.to_field: "0x2",
      self.from_field: "0x1",
      "token": "0x1",
      "value": 1,
      "block_id": 2,
    }, refresh=True)

    received_incomes = self._call_method("0x1", 1)
    self.assertSequenceEqual({"0x2": 2}, received_incomes)

class InTokenTransactionsTestCase(InOutTransactionsTestCase, unittest.TestCase):
  to_field = "to"
  from_field = "from"
  block_field = "block_id"
  def _call_method(self, *args):
    return get_token_incomes(*args)

class OutTokenTransactionsTestCase(InOutTransactionsTestCase, unittest.TestCase):
  to_field = "from"
  from_field = "to"
  block_field = "block_id"
  def _call_method(self, *args):
    return get_token_outcomes(*args)

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
    with patch('api.server.get_token_incomes', test_incomes_mock), \
         patch('api.server.get_token_outcomes', test_outcomes_mock):
      process = Mock(
        incomes=test_incomes_mock,
        outcomes=test_outcomes_mock
      )

      received_balances = get_token_balances("0x1", 1)
      print(received_balances)

      process.assert_has_calls([
        call.incomes("0x1", 1),
        call.outcomes("0x1", 1)
      ])
      self.assertSequenceEqual(test_balances, received_balances)

  def test_get_balances_api(self):
    test_balances = {
      "0x1": 1,
      "0x2": 2,
      "0x3": 3
    }
    test_balances_mock = MagicMock(return_value=test_balances)
    with patch("api.server.get_token_balances", test_balances_mock):
      received_balances = self.app.get("/token_balances?token=0x0&block=1").json
      test_balances_mock.assert_called_with("0x0", 1)
      self.assertSequenceEqual(test_balances, received_balances)

  def test_get_balances_api_empty_block(self):
    test_balances_mock = MagicMock(return_value={})
    with patch("api.server.get_token_balances", test_balances_mock):
      self.app.get("/token_balances?token=0x0")
      test_balances_mock.assert_called_with("0x0", None)

TEST_CONTRACTS_INDEX = 'test-ethereum-contracts'
TEST_TOKEN_TRANSACTIONS_INDEX = 'test-ethereum-token-transactions'