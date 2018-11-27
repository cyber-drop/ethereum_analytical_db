import requests
from clients.custom_clickhouse import CustomClickhouse
from config import INDICES, PARITY_HOSTS
import datetime
from datetime import date
from pyelasticsearch import bulk_chunks
from tqdm import *
import numpy as np
import pandas as pd
from web3 import Web3, HTTPProvider
from utils import ClickhouseContractTransactionsIterator

MOVING_AVERAGE_WINDOW = 5
DAYS_LIMIT = 2000

class ClickhouseTokenPrices(ClickhouseContractTransactionsIterator):
  doc_type = 'token'
  block_prefix = 'prices_extracted'
  '''
  Extract token prices from Coinmarketcap and CryptoCompare and save in Elasticsearch
  '''
  def __init__(self, indices=INDICES, parity_host=PARITY_HOSTS[0][-1]):
    self.indices = indices
    self.client = CustomClickhouse()
    self.web3 = Web3(HTTPProvider(parity_host))

  def _chunks(self, l, n):
    '''
    Break list into list of n list of equal size

    Parameters
    ----------
    l: list
      Lisymbol_listst that will be breaked into chunks
    n: int
      Number of chunks
    '''
    for i in range(0, len(l), n):
        yield l[i:i+n]

  def _to_usd(self, value, price):
    '''
    Convert token BTC price to USD price

    Parameters
    ----------
    value: float
      Token price
    price: float
      BTC price in USD

    Returns
    -------
    float
      Token price in USD
    '''
    return value * price

  def _from_usd(self, value, price):
    '''
    Convert token USD price to BTC or ETH price

    Parameters
    ----------
    value: float
      Token price
    price: float
      USD price in BTC or ETH

    Returns
    -------
    float
      Token price in BTC or ETH
    '''
    return value / price

  def _iterate_cc_tokens(self):
    '''
    Iterate over token contracts that are listed on Cryptocompare

    Returns
    -------
    generator
      Generator that iterates over listed token contracts in Elasticsearch
    '''
    # return self.client.iterate(index=self.indices["contract"], query="WHERE address IS NOT NULL", fields=["address"])
    return self._iterate_contracts(partial_query='WHERE standard_erc20 = 1', fields=["address"])

  def _get_cc_tokens(self):
    '''
    Extract list of tokens listed on Cryptocompare

    Returns
    -------
    list
      List of listed tokens contracts
    '''
    tokens = [token_chunk for token_chunk in self._iterate_cc_tokens()]
    token_list = [t['_source'] for token_chunk in tokens for t in token_chunk]
    return token_list

  def _get_prices_for_fsyms(self, symbol_list):
    '''
    Extract tokens historical prices from CryptoCompare
    
    Parameters
    ----------
    symbol_list: list
      List of token symbols

    Returns
    -------
    list
      List of tokens prices
    '''
    fsyms = ','.join(symbol_list)
    url = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms={}&tsyms=BTC'.format(fsyms)
    try:
      prices = requests.get(url).json()
    except:
      prices = None
    return prices

  def _make_multi_prices_req(self, tokens):
    '''
    Extract tokens BTC prices from CryptoCompare

    Parameters
    ----------
    tokens: list
      List of tokens

    Returns
    -------
    list
      List of tokens prices
    '''
    token_list_chunks = list(self._chunks(tokens, 60))
    all_prices = []
    for symbols in token_list_chunks:
      prices = self._get_prices_for_fsyms(symbols)
      if prices != None:
        prices = [{'address': key, 'BTC': float('{:0.10f}'.format(prices[key]['BTC']))} for key in prices.keys()]
        all_prices.append(prices)
    all_prices = [price for prices in all_prices for price in prices]
    return all_prices

  def _get_multi_prices(self):
    '''
    Extract listed tokens current prices from CryptoCompare

    Returns
    -------
    list
      List of tokens prices
    '''
    now = datetime.datetime.now()
    self._get_btc_eth_current_prices()
    token_syms = [token['address'] for token in self._get_cc_tokens()]
    prices = self._make_multi_prices_req(token_syms)
    for price in prices:
      price['USD'] = float('{:0.10f}'.format(self._to_usd(price['BTC'], self.btc_price)))
      price['ETH'] = float('{:0.10f}'.format(self._from_usd(price['USD'], self.eth_price)))
      price['timestamp'] = now.strftime("%Y-%m-%d")
    return prices

  def _construct_bulk_insert_ops(self, docs):
    '''
    Iterate over docs and create document-inserting operations used in bulk insert

    Parameters
    ----------
    docs: list
      List of dictionaries with new data
    '''
    for doc in docs:
      doc["id"] = doc['address']+'_'+doc['timestamp'].strftime("%Y-%m-%d")

  def _insert_multiple_docs(self, docs, index_name):
    ''' 
    Index multiple documents simultaneously

    Parameters
    ----------
    docs: list
      List of dictionaries with new data
    doc_type: str 
      Type of inserted documents
    index_name: str
      Name of the index that contains inserted documents
    '''
    for chunk in bulk_chunks(docs, docs_per_chunk=1000):
      self._construct_bulk_insert_ops(chunk)
      self.client.bulk_index(index=index_name, docs=chunk)

  def get_recent_token_prices(self):
    '''
    Extract listed tokens current prices from CryptoCompare and saves in Elasticsearch
    '''
    prices = self._get_multi_prices()
    self._insert_multiple_docs(prices, 'price', self.indices['price'])

  def _set_moving_average(self, prices, window_size=MOVING_AVERAGE_WINDOW):
    prices_stack = []
    for price in prices:
      prices_stack.append(price["close"])
      if len(prices_stack) == window_size:
        price["average"] = np.mean(prices_stack)
        prices_stack.pop(0)
      else:
        price["average"] = price["close"]

  def _process_hist_prices(self, prices):
    '''
    Convert tokens historical prices from BTC to USD and ETH

    Parameters
    ----------
    prices: list
      List of tokens prices

    Returns
    -------
    list
      List of tokens prices
    '''
    points = []
    self._set_moving_average(prices)
    for price in prices:
      point = {}
      point['BTC'] = price["average"]
      point['BTC'] = float('{:0.10f}'.format(point['BTC']))
      point['timestamp'] = datetime.datetime.fromtimestamp(price['time'])
      point['address'] = price['address']
      points.append(point)
    return points

  def _make_historical_prices_req(self, address, days_count):
    '''
    Make call to CryptoCompare API to extract token historical data

    Parameters
    ----------
    symbol: str
      Token symbol
    days_count: int
      Days limit

    Returns
    -------
    list
      List of token historical prices
    '''
    symbol = self._get_symbol_by_address(address)
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym=BTC&limit={}'.format(symbol, days_count)
    try:
      res = requests.get(url).json()
      for point in res['Data']:
        point['address'] = address
      return res['Data']
    except:
      print("No exchange rate for {}".format(symbol))
      return

  def _get_last_avail_price_date(self):
    '''
    Get last price available in Elasticsearch token_price index

    Returns
    -------
    string
      Timestamp of last available date or 2013-01-01 if there are no prices in index
    '''
    return self.client.send_sql_request('SELECT MAX(timestamp) FROM {}'.format(self.indices['price']))

  def _convert_btc_eth_prices(self, price):
    '''
    Convert BTC and ETH prices into usable format

    Parameters
    ----------
    price: dict
      A dictionary with raw data from cryptocompare

    Returns
    -------
    dict
      A dictionary with converted price data
    '''
    point = {}
    point['USD'] = (price['open'] + price['close']) / 2
    point['date'] = datetime.datetime.fromtimestamp(price['time']).strftime("%Y-%m-%d")
    return point

  def _get_btc_eth_prices(self):
    '''
    Extract BTC and ETH historcial data and convert it into usable format
    '''
    btc_prices = requests.get('https://min-api.cryptocompare.com/data/histoday?fsym=BTC&tsym=USD&allData=true').json()['Data']
    eth_prices = requests.get('https://min-api.cryptocompare.com/data/histoday?fsym=ETH&tsym=USD&allData=true').json()['Data']
    btc_prices = [self._convert_btc_eth_prices(price) for price in btc_prices]
    eth_prices = [self._convert_btc_eth_prices(price) for price in eth_prices]
    btc_prices_dict = {price['date']: price['USD'] for price in btc_prices}
    eth_prices_dict = {price['date']: price['USD'] for price in eth_prices}
    self.btc_prices = btc_prices_dict
    self.eth_prices = eth_prices_dict

  def _get_days_count(self, now, last_price_date, limit=DAYS_LIMIT):
    '''
    Count number of days for that prices are unavailable

    Parameters
    ----------
    now: str
      Current date
    last_price_date: str
      Timestamp of last available price

    Returns
    -------
    int
      Number of days
    '''
    days_count = (now - last_price_date).days + 1
    return min(days_count, DAYS_LIMIT)

  def _get_symbol_abi(self, output_type):
    return [{
      "constant": True,
      "inputs": [],
      "name": "symbol",
      "outputs": [
        {
          "name": "",
          "type": output_type
        }
      ],
      "payable": False,
      "stateMutability": "view",
      "type": "function"
    }]

  def _get_symbol_by_address(self, address):
    address = self.web3.toChecksumAddress(address)
    symbols = {}
    for output_type in ['string', 'bytes32']:
      contract = self.web3.eth.contract(abi=self._get_symbol_abi(output_type), address=address)
      try:
        symbols[output_type] = contract.functions.symbol().call()
      except Exception as e:
        print(e)
        pass
    if 'string' in symbols:
      return symbols['string']
    else:
      return symbols.get('bytes32', "".encode('utf-8')).decode('utf-8').rstrip('\0')

  def _get_historical_multi_prices(self):
    '''
    Extract historical token prices from CryptoCompare

    Returns
    -------
    list
      List ot token historical prices
    '''
    token_addresses = [
      token['address']
      for token in self._get_cc_tokens()
    ]
    now = datetime.datetime.now()
    last_price_date = self._get_last_avail_price_date()
    days_count = self._get_days_count(now, last_price_date)
    prices = []
    for token in tqdm(token_addresses):
      price = self._make_historical_prices_req(token, days_count)
      if price != None:
        price = self._process_hist_prices(price) 
        prices.append(price)
      else:
        continue
    prices = [p for price in prices for p in price]
    return prices

  def get_prices_within_interval(self):
    '''
    Extract historcial token prices and then add to this prices data from Coinmarketcap

    This function is an entry point for extract-prices operation
    '''
    prices = self._get_historical_multi_prices()
    if prices != None:
      self._insert_multiple_docs(prices, self.indices['price'])
