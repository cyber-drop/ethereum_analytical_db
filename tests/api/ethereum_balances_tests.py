import api.server as server
from api.server import get_ethereum_incomes, \
  get_internal_ethereum_incomes, \
  get_ethereum_outcomes, \
  get_internal_ethereum_outcomes, \
  get_ethereum_balances, \
  _split_range
import unittest
from tests.test_utils import TestElasticSearch
from unittest.mock import MagicMock, Mock, patch, call

class EthereumTransactionsTestCase():
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_fast_index(TEST_TRANSACTIONS_INDEX, self.doc_type)
    server.app.config.update({
      self.index_key: TEST_TRANSACTIONS_INDEX,
    })

  def test_get_ethereum_incomes(self):
    self.client.index(index=TEST_TRANSACTIONS_INDEX, doc_type=self.doc_type, doc={
      self.to_field: "0x1",
      self.from_field: "0x2",
      "value": self._convert_value(1000)
    }, refresh=True)
    received_balances = self._get_state()
    print(received_balances)
    self.assertSequenceEqual({"0x1": 1000}, received_balances)

  def test_get_ethereum_incomes_for_range(self):
    self.client.index(index=TEST_TRANSACTIONS_INDEX, doc_type=self.doc_type, doc={
      self.to_field: "0x1",
      self.from_field: "0x2",
      "value": self._convert_value(100),
      "blockNumber": 1
    }, refresh=True)
    self.client.index(index=TEST_TRANSACTIONS_INDEX, doc_type=self.doc_type, doc={
      self.to_field: "0x1",
      self.from_field: "0x2",
      "value": self._convert_value(200),
      "blockNumber": 2
    }, refresh=True)
    self.client.index(index=TEST_TRANSACTIONS_INDEX, doc_type=self.doc_type, doc={
      self.to_field: "0x1",
      self.from_field: "0x2",
      "value": self._convert_value(300),
      "blockNumber": 3
    }, refresh=True)
    received_balances = self._get_state(start=2, end=2)
    self.assertSequenceEqual({"0x1": 200}, received_balances)

  def test_get_ethereum_incomes_ignore_zeros(self):
    self.client.index(index=TEST_TRANSACTIONS_INDEX, doc_type=self.doc_type, doc={
      self.to_field: "0x1",
      self.from_field: "0x2",
      "value": self._convert_value(0)
    }, refresh=True)
    received_balances = self._get_state()
    assert not received_balances

class InEthereumExternalTransactionsTestCase(EthereumTransactionsTestCase, unittest.TestCase):
  index_key = "transaction"
  doc_type = "tx"
  to_field = "to"
  from_field = "from"

  def _convert_value(self, value):
    return value

  def _get_state(self, *args, **kwargs):
    return get_ethereum_incomes(*args, **kwargs)

class OutEthereumExternalTransactionsTestCase(EthereumTransactionsTestCase, unittest.TestCase):
  index_key = "transaction"
  doc_type = "tx"
  to_field = "from"
  from_field = "to"

  def _convert_value(self, value):
    return value

  def _get_state(self, *args, **kwargs):
    return get_ethereum_outcomes(*args, **kwargs)

class InEthereumInternalTransactionsTestCase(EthereumTransactionsTestCase, unittest.TestCase):
  index_key = "internal_transaction"
  doc_type = "itx"
  to_field = "to"
  from_field = "from"

  def _convert_value(self, value):
    return hex(int(value * 1e18))

  def _get_state(self, *args, **kwargs):
    return get_internal_ethereum_incomes(*args, **kwargs)

  def test_get_ethereum_incomes_for_big_values(self):
    self.client.index(index=TEST_TRANSACTIONS_INDEX, doc_type=self.doc_type, doc={
      self.to_field: "0x1",
      self.from_field: "0x2",
      "value": "0x2b5ea42903702b000"
    }, refresh=True)
    received_balances = get_internal_ethereum_incomes()
    self.assertSequenceEqual({"0x1": 50.001851}, received_balances)

class OutEthereumInternalTransactionsTestCase(EthereumTransactionsTestCase, unittest.TestCase):
  index_key = "internal_transaction"
  doc_type = "itx"
  to_field = "from"
  from_field = "to"

  def _convert_value(self, value):
    return hex(int(value * 1e18))

  def _get_state(self, *args, **kwargs):
    return get_internal_ethereum_outcomes(*args, **kwargs)

  def test_get_ethereum_incomes_for_big_values(self):
    self.client.index(index=TEST_TRANSACTIONS_INDEX, doc_type=self.doc_type, doc={
      self.to_field: "0x1",
      self.from_field: "0x2",
      "value": "0x2b5ea42903702b000"
    }, refresh=True)
    received_balances = get_internal_ethereum_incomes()
    print(received_balances)
    self.assertSequenceEqual({"0x2": 50.001851}, received_balances)

class APITestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_fast_index(TEST_TRANSACTIONS_INDEX)
    server.app.config.update({
      "token_tx": TEST_TRANSACTIONS_INDEX
    })
    self.app = server.app.test_client()

  def test_split_range(self):
    ranges = _split_range(1, 1000, size=200)
    self.assertSequenceEqual([(1, 200), (201, 400), (401, 600), (601, 800), (801, 1000)], ranges)

  def test_split_range_last_range(self):
    ranges = _split_range(1, 10, size=20)
    self.assertSequenceEqual([(1, 10)], ranges)

  def test_get_ethereum_balances(self):
    test_block = 124
    test_income = {"0x1": 100, "0x2": 10, "0x3": 1}
    test_outcome = {"0x1": 1, "0x2": 10}
    test_internal_income = {"0x2": 1}
    test_internal_outcome = {"0x1": 2}
    test_income_mock = MagicMock(return_value=test_income)
    test_outcome_mock = MagicMock(return_value=test_outcome)
    test_internal_income_mock = MagicMock(return_value=test_internal_income)
    test_internal_outcome_mock = MagicMock(return_value=test_internal_outcome)
    test_split_range = MagicMock(return_value=[(0, test_block)])

    with patch('api.server.get_ethereum_incomes', test_income_mock), \
         patch('api.server.get_ethereum_outcomes', test_outcome_mock), \
         patch("api.server.get_internal_ethereum_incomes", test_internal_income_mock), \
         patch("api.server.get_internal_ethereum_outcomes", test_internal_outcome_mock), \
         patch('api.server._split_range', test_split_range):
      get_ethereum_balances(test_block)

      test_income_mock.assert_called_with(0, test_block)
      test_outcome_mock.assert_called_with(0, test_block)
      test_internal_income_mock.assert_called_with(0, test_block)
      test_internal_outcome_mock.assert_called_with(0, test_block)
      assert get_ethereum_balances(test_block) == {
        "0x1": 100 - 1 - 2,
        "0x2": 10 - 10 + 1,
        "0x3": 1
      }

  def test_get_ethereum_balances_pagination(self):
    test_block = 5
    test_chunks = [(0, 1), (2, 3), (4, 5)]
    test_income_mock = MagicMock(side_effect=[{"0x1": 1}, {"0x2": 1}, {"0x3": 1}])
    test_outcome_mock = MagicMock(return_value={})
    test_internal_income_mock = MagicMock(return_value={})
    test_internal_outcome_mock = MagicMock(return_value={})
    test_split_range = MagicMock(return_value=test_chunks)

    with patch('api.server.get_ethereum_incomes', test_income_mock), \
         patch('api.server.get_ethereum_outcomes', test_outcome_mock), \
         patch("api.server.get_internal_ethereum_incomes", test_internal_income_mock), \
         patch("api.server.get_internal_ethereum_outcomes", test_internal_outcome_mock), \
         patch('api.server._split_range', test_split_range):
      balances = get_ethereum_balances(test_block)

      test_split_range.assert_called_with(0, test_block, 10000)
      for mock in [test_income_mock, test_outcome_mock, test_internal_income_mock, test_internal_outcome_mock]:
        for chunk in test_chunks:
          mock.assert_any_call(chunk[0], chunk[1])
      assert balances == {
        "0x1": 1,
        "0x2": 1,
        "0x3": 1
      }

  def test_get_ethereum_balances_api(self):
    test_balances = {
      "0x1": 1,
      "0x2": 2,
      "0x3": 3
    }
    test_balances_mock = MagicMock(return_value=test_balances)
    with patch("api.server.get_ethereum_balances", test_balances_mock):
      received_balances = self.app.get("/ethereum_balances?block=1").json
      test_balances_mock.assert_called_with(1)
      self.assertSequenceEqual(test_balances, received_balances)

  def test_get_balances_api_empty_block(self):
    test_balances_mock = MagicMock(return_value={})
    with patch("api.server.get_ethereum_balances", test_balances_mock):
      self.app.get("/ethereum_balances")
      test_balances_mock.assert_called_with(None)

TEST_TRANSACTIONS_INDEX = "test-ethereum-transactions"
TEST_INTERNAL_TRANSACTIONS_INDEX = "test-ethereum-internal-transactions"