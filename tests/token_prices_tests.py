import unittest
from operations.token_prices import ClickhouseTokenPrices
from unittest import mock
from tests.test_utils import TestClickhouse, parity
from datetime import datetime, timedelta
import config

class ClickhouseTokenPricesTestCase():
  def setUp(self):
    config.PROCESSED_CONTRACTS.clear()
    self.client = TestClickhouse()
    self.indices = {
      'contract': TEST_CONTRACT_INDEX,
      'contract_block': TEST_CONTRACT_BLOCK_INDEX,
      'price': TEST_PRICES_INDEX
    }
    self.client.prepare_indices(self.indices)
    self.token_prices = ClickhouseTokenPrices(self.indices, TEST_PARITY_URL)

  def test_iterate_tokens_from_whitelist(self):
    test_contracts = [{
      "id": 1,
      "address": "0x1",
      "standard_erc20": 1
    }, {
      "id": 2,
      "address": "0x2",
      "standard_erc20": 1
    }]
    self.client.bulk_index(index=TEST_CONTRACT_INDEX, docs=test_contracts)
    config.PROCESSED_CONTRACTS.append("0x1")

    contracts = self.token_prices._get_cc_tokens()

    self.assertCountEqual([contract["address"] for contract in contracts], ["0x1"])

  def test_moving_average(self):
    test_prices = [
      {"close": 2},
      {"close": 3},
      {"close": 4},
      {"close": 5},
      {"close": 4},
      {"close": 200},
      {"close": 5},
    ]

    self.token_prices._set_moving_average(test_prices, window_size=5)

    self.assertSequenceEqual([price["average"] for price in test_prices], [
      2,
      3,
      4,
      5,
      (2 + 3 + 4 + 5 + 4) / 5,
      (3 + 4 + 5 + 4 + 200) / 5,
      (4 + 5 + 4 + 200 + 5) / 5,
    ])

  def test_moving_average_usage(self):
    test_prices = [{
      "open": 1,
      "close": 2,
      "average": 10,
      'time': 1415463675,
      'address': "0x1"
    }]
    self.token_prices.btc_prices = {
      '2014-11-08': 0
    }
    self.token_prices.eth_prices = {
      '2014-11-08': 0
    }
    self.token_prices._set_moving_average = mock.MagicMock()

    result = self.token_prices._process_hist_prices(test_prices)

    self.token_prices._set_moving_average.assert_any_call(test_prices)
    self.assertSequenceEqual([price["BTC"] for price in result], [10])

  def test_get_last_avail_price_date(self):
    test_current_datetime = datetime.today()
    test_prices = [{
      "id": 1,
      "timestamp": test_current_datetime - timedelta(days=10)
    }, {
      "id": 2,
      "timestamp": test_current_datetime
    }]

    self.client.bulk_index(index=TEST_PRICES_INDEX, docs=test_prices)
    last_date = self.token_prices._get_last_avail_price_date()
    assert (last_date - test_current_datetime).days < 1

  def test_get_last_day_empty_index(self):
    last_date = self.token_prices._get_last_avail_price_date()
    assert (last_date - datetime(1970, 1, 1)).days < 1

  @parity
  def test_get_symbol_by_address(self):
    test_contracts = {
      "0xf230b790e05390fc8295f4d3f60332c93bed42e2": "TRX",
      "0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0": "EOS",
      "0xb5a5f22694352c15b00323844ad545abb2b11028": ""
    }
    for address, symbol in test_contracts.items():
      result = self.token_prices._get_symbol_by_address(address)
      assert symbol == result

  def test_get_days_count(self):
    test_dates = [
      datetime.now() - timedelta(days=10),
      datetime.now() - timedelta(days=3000)
    ]
    test_limit = 2000
    test_days = [11, test_limit]
    test_last_date = datetime.now()
    for i, date in enumerate(test_dates):
      result = self.token_prices._get_days_count(test_last_date, date, limit=test_limit)
      assert test_days[i] == result

  def test_process(self):
    test_contracts = ["0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0"]
    self.client.bulk_index(
      index=self.indices["contract"],
      docs=[{"address": contract, "id": contract, "standard_erc20": True} for contract in test_contracts]
    )
    self.token_prices.get_prices_within_interval()
    prices = self.client.search(index=TEST_PRICES_INDEX, fields=["BTC", "address", "timestamp"])
    print(prices)
    assert len(prices)

TEST_PARITY_URL = "http://localhost:8545"
TEST_PRICES_INDEX = 'test_token_prices'
TEST_CONTRACT_INDEX = 'test_ethereum_contract'
TEST_CONTRACT_BLOCK_INDEX = 'test_ethereum_contract_block'
TEST_TOKEN_SYMBOLS = ['AE', 'FND', 'CPAY', 'SEXC']
TEST_ADDRESSES = ['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', '0x4df47b4969b2911c966506e3592c41389493953b', '0x0ebb614204e47c09b6c3feb9aaecad8ee060e23e', '0x2567c677473d110d75a8360c35309e63b1d52429']
TEST_RES = {"AE":{"BTC":0.0004365},"FND":{"BTC":0.00001584},"CPAY":{"BTC":0.00000634}}
