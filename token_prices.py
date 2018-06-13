import requests
from custom_elastic_search import CustomElasticSearch
from config import INDICES
import datetime
from pyelasticsearch import bulk_chunks

class TokenPrices:
  def __init__(self, elasticsearch_indices=INDICES, elasticsearch_host='http://localhost:9200'):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)

  def _chunks(self, l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]

  def _to_usd(self, value, price):
    return value * price

  def _from_usd(self, value, price):
    return value / price

  def _iterate_cc_tokens(self):
    return self.client.iterate(self.indices['contract'], 'contract', 'cc_sym:*')

  def _get_cc_tokens(self):
    tokens = [token_chunk for token_chunk in self._iterate_cc_tokens()]
    token_list = [t['_source'] for token_chunk in tokens for t in token_chunk]
    return token_list

  def _get_prices_for_fsyms(self, symbol_list):
    fsyms = ','.join(symbol_list)
    url = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms={}&tsyms=BTC'.format(fsyms)
    try:
      prices = requests.get(url).json()
    except:
      prices = None
    return prices

  def _make_multi_prices_req(self, tokens):
    token_list_chunks = list(self._chunks(tokens, 60))
    all_prices = []
    for symbols in token_list_chunks:
      prices = self._get_prices_for_fsyms(symbols)
      if prices != None:
        prices = [{'token': key, 'BTC': float('{:0.10f}'.format(prices[key]['BTC']))} for key in prices.keys()]
        all_prices.append(prices)
    all_prices = [price for prices in all_prices for price in prices]
    return all_prices

  def _get_btc_eth_current_prices(self):
    url = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms=BTC,ETH&tsyms=USD'
    res = requests.get(url).json()
    self.btc_price = res['BTC']['USD']
    self.eth_price = res['ETH']['USD']

  def _get_multi_prices(self):
    now = datetime.datetime.now()
    self._get_btc_eth_current_prices()
    token_syms = [token['cc_sym'] for token in self._get_cc_tokens()]
    prices = self._make_multi_prices_req(token_syms)
    for price in prices:
      price['USD'] = float('{:0.10f}'.format(self._to_usd(price['BTC'], self.btc_price)))
      price['ETH'] = float('{:0.10f}'.format(self._from_usd(price['USD'], self.eth_price)))
      price['timestamp'] = now.strftime("%Y-%m-%d")
    return prices

  def _construct_bulk_insert_ops(self, docs):
    for doc in docs:
      yield self.client.index_op(doc)

  def _insert_multiple_docs(self, docs, doc_type, index_name):
    for chunk in bulk_chunks(self._construct_bulk_insert_ops(docs), docs_per_chunk=1000):
      self.client.bulk(chunk, doc_type=doc_type, index=index_name, refresh=True)

  def get_recent_token_prices(self):
    prices = self._get_multi_prices()
    self._insert_multiple_docs(prices, 'price', self.indices['token_prices'])
