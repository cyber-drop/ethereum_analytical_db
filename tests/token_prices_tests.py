import unittest
from token_prices import TokenPrices
from test_utils import TestElasticSearch
from unittest.mock import patch, Mock

class TokenPricesTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.token_prices = TokenPrices({'contract': TEST_CONTRACT_INDEX, 'token_prices': TEST_PRICES_INDEX})
    self.client.recreate_index(TEST_PRICES_INDEX)
    self.client.recreate_index(TEST_CONTRACT_INDEX)

  def test_get_recent_token_prices(self):
    for sym in TEST_TOKEN_SYMBOLS:
      self.client.index(TEST_CONTRACT_INDEX, 'contract', {'cc_sym': sym, 'cc_listed': True}, refresh=True)
    mock_get_patcher = patch('token_prices.requests.get')
    mock_get = mock_get_patcher.start()
    mock_get.return_value = Mock(status_code = 200)
    mock_get.return_value.json.return_value = TEST_RES
    self.token_prices.get_recent_token_prices()
    mock_get_patcher.stop()
    prices = self.token_prices._iterate_prices()
    prices = [p['_source'] for price in prices for p in price]
    prices_fsyms = [p['fsym'] for p in prices]
    prices_usd = [p['USD'] for p in prices]
    self.assertCountEqual([3.32, 0.1199, 0.04797], prices_usd)

TEST_PRICES_INDEX = 'test-token-prices'
TEST_CONTRACT_INDEX = 'test-ethereum-contract'
TEST_TOKEN_SYMBOLS = ['AE', 'FND', 'CPAY']
TEST_RES = {"AE":{"ETH":0.005511,"BTC":0.0004365,"USD":3.32},"FND":{"ETH":0.0002,"BTC":0.00001584,"USD":0.1199},"CPAY":{"ETH":0.00008,"BTC":0.00000634,"USD":0.04797}}