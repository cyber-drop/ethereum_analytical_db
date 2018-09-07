import unittest
from token_prices import TokenPrices
from tests.test_utils import TestElasticSearch
from unittest import mock

def mocked_requests_get(*args, **kwargs):
  class MockResponse:
    def __init__(self, json_data, status_code):
      self.json_data = json_data
      self.status_code = status_code
    def json(self):
      return self.json_data
  if args[0] == 'https://min-api.cryptocompare.com/data/pricemulti?fsyms=BTC,ETH&tsyms=USD':
    return MockResponse({"BTC":{"USD":6508.66},"ETH":{"USD":470.14}}, 200)
  else:
    return MockResponse(TEST_RES, 200)
  return MockResponse(None, 404)

class TokenPricesTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.token_prices = TokenPrices({'contract': TEST_CONTRACT_INDEX, 'token_price': TEST_PRICES_INDEX})
    self.client.recreate_index(TEST_PRICES_INDEX)
    self.client.recreate_index(TEST_CONTRACT_INDEX)
  def _iterate_prices(self):
    return self.token_prices.client.iterate(TEST_PRICES_INDEX, 'price', 'token:*')

  @mock.patch('requests.get', side_effect=mocked_requests_get)
  def test_get_recent_token_prices(self, mock_get):
    for sym in TEST_TOKEN_SYMBOLS:
      self.client.index(TEST_CONTRACT_INDEX, 'contract', {'cc_sym': sym}, refresh=True)
    self.token_prices.get_recent_token_prices()
    prices = self._iterate_prices()
    prices = [p['_source'] for price in prices for p in price]
    prices_fsyms = [p['token'] for p in prices]
    prices_usd = [p['USD'] for p in prices]
    self.assertCountEqual([0.1030971744, 0.0412649044, 2.84103009], prices_usd)

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

    self.token_prices._set_moving_average(test_prices)

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
      'token': "JAT"
    }]
    self.token_prices.btc_prices = {
      '2014-11-08': 0
    }
    self.token_prices.eth_prices = {
      '2014-11-08': 0
    }
    self.token_prices._set_moving_average = mock.MagicMock()
    self.token_prices._to_usd = mock.MagicMock(return_value=0)
    self.token_prices._from_usd = mock.MagicMock(return_value=0)

    result = self.token_prices._process_hist_prices(test_prices)

    self.token_prices._set_moving_average.assert_any_call(test_prices)
    self.assertSequenceEqual([price["BTC"] for price in result], [10])


TEST_PRICES_INDEX = 'test-token-prices'
TEST_CONTRACT_INDEX = 'test-ethereum-contract'
TEST_TOKEN_SYMBOLS = ['AE', 'FND', 'CPAY', 'SEXC']
TEST_ADDRESSES = ['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', '0x4df47b4969b2911c966506e3592c41389493953b', '0x0ebb614204e47c09b6c3feb9aaecad8ee060e23e', '0x2567c677473d110d75a8360c35309e63b1d52429']
TEST_RES = {"AE":{"BTC":0.0004365},"FND":{"BTC":0.00001584},"CPAY":{"BTC":0.00000634}}
