import requests
from custom_elastic_search import CustomElasticSearch
from config import INDICES
import datetime
from datetime import date
from pyelasticsearch import bulk_chunks
from tqdm import *
import time

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
    self._insert_multiple_docs(prices, 'price', self.indices['token_price'])

  def _process_hist_prices(self, prices):
    points = []
    for price in prices:
      point = {}
      point['BTC'] = (price['open'] + price['close']) / 2
      point['BTC'] = float('{:0.10f}'.format(point['BTC']))
      point['timestamp'] = datetime.datetime.fromtimestamp(price['time']).strftime("%Y-%m-%d")
      point['token'] = price['token']
      point['USD'] = self._to_usd(point['BTC'], self.btc_prices[point['timestamp']])
      point['USD'] = float('{:0.10f}'.format(point['USD']))
      if point['timestamp'] in self.eth_prices.keys():
        point['ETH'] = self._from_usd(point['USD'], self.eth_prices[point['timestamp']])
        point['ETH'] = float('{:0.10f}'.format(point['ETH']))
      point['source'] = 'cryptocompare'
      points.append(point)
    return points

  def _make_historical_prices_req(self, symbol, days_count):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym=BTC&limit={}'.format(symbol, days_count)
    time.sleep(0.5)
    try:
      res = requests.get(url).json()
      for point in res['Data']:
        point['token'] = symbol
      return res['Data']
    except:
      return

  def _get_last_avail_price_date(self):
    query = {
      "from" : 0, "size" : 1,
      'sort': {
        'timestamp': {'order': 'desc'}
      }
    }
    res = self.client.send_request('GET', [self.indices['token_price'], 'price', '_search'], query, {})['hits']['hits']
    last_date = res[0]['_source']['timestamp'] if len(res) > 0 else '2013-01-01'
    last_date = last_date.split('-')
    return last_date

  def _convert_btc_eth_prices(self, price):
    point = {}
    point['USD'] = (price['open'] + price['close']) / 2
    point['date'] = datetime.datetime.fromtimestamp(price['time']).strftime("%Y-%m-%d")
    return point

  def _get_btc_eth_prices(self):
    btc_prices = requests.get('https://min-api.cryptocompare.com/data/histoday?fsym=BTC&tsym=USD&allData=true').json()['Data']
    eth_prices = requests.get('https://min-api.cryptocompare.com/data/histoday?fsym=ETH&tsym=USD&allData=true').json()['Data']
    btc_prices = [self._convert_btc_eth_prices(price) for price in btc_prices]
    eth_prices = [self._convert_btc_eth_prices(price) for price in eth_prices]
    btc_prices_dict = {price['date']: price['USD'] for price in btc_prices}
    eth_prices_dict = {price['date']: price['USD'] for price in eth_prices}
    self.btc_prices = btc_prices_dict
    self.eth_prices = eth_prices_dict

  def _get_days_count(self, now, last_price_date):
    start_date = date(int(now[0]), int(now[1]), int(now[2]))
    end_date = date(int(last_price_date[0]), int(last_price_date[1]), int(last_price_date[2]))
    days_count = (start_date - end_date).days + 1
    return days_count

  def _get_historical_multi_prices(self):
    self._get_btc_eth_prices()
    token_syms = [token['cc_sym'] for token in self._get_cc_tokens()]
    now = datetime.datetime.now().strftime("%Y-%m-%d").split('-')
    last_price_date = self._get_last_avail_price_date()
    if last_price_date == now:
      return
    days_count = self._get_days_count(now, last_price_date)
    prices = []
    for i in tqdm(range(len(token_syms))):
      price = self._make_historical_prices_req(token_syms[i], days_count)
      prices.append(price)
    prices = [price for price in prices if price != None]
    prices = [self._process_hist_prices(price) for price in prices]
    prices = [p for price in prices for p in price]
    return prices

  def get_prices_within_interval(self):
    prices = self._get_historical_multi_prices()
    if prices != None:
      self._insert_multiple_docs(prices, 'price', self.indices['token_price'])
