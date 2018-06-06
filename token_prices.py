import requests
from custom_elastic_search import CustomElasticSearch
from config import INDICES
import datetime
import time
from tqdm import *
from os import listdir
from os.path import isfile, join
from decimal import *
import pandas as pd 
import itertools
from pyelasticsearch import bulk_chunks

class TokenPrices:
  def __init__(self, elasticsearch_indices=INDICES, elasticsearch_host='http://localhost:9200'):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)

  def _chunks(self, l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]

  def _iterate_cc_tokens(self):
    return self.client.iterate(self.indices['contract'], 'contract', 'cc_listed:true')

  def _get_cc_tokens(self):
    tokens = [token_chunk for token_chunk in self._iterate_cc_tokens()]
    token_list = [t['_source'] for token_chunk in tokens for t in token_chunk]
    return token_list

  def _get_prices_for_fsyms(self, symbol_list):
    fsyms = ','.join(symbol_list)
    url = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms={}&tsyms=ETH,BTC,USD'.format(fsyms)
    try:
      prices = requests.get(url).json()
    except:
      prices = None
    return prices

  def _make_multi_prices_req(self, tokens):
    token_list_chunks = list(self._chunks(tokens, 60))
    all_prices = {}
    for symbols in token_list_chunks:
      prices = self._get_prices_for_fsyms(symbols)
      if prices != None:
        all_prices.update(prices)
    return all_prices

  def _get_multi_prices(self):
    now = datetime.datetime.now()
    token_syms = [token['cc_sym'] for token in self._get_cc_tokens()]
    res = self._make_multi_prices_req(token_syms)
    prices = [{'fsym': key, 'ETH': res[key]['ETH'], 'BTC': res[key]['BTC'], 'USD': res[key]['USD'], 'date': now.strftime("%Y-%m-%d")} for key in res.keys()]
    return prices

  def _construct_bulk_insert_ops(self, docs):
    for doc in docs:
      yield self.client.index_op(doc)

  def _insert_multiple_docs(self, docs, doc_type, index_name):
    for chunk in bulk_chunks(self._construct_bulk_insert_ops(docs), docs_per_chunk=1000):
      self.client.bulk(chunk, doc_type=doc_type, index=index_name, refresh=True)

  def get_recent_token_prices(self):
    prices = self._get_multi_prices()
    self._insert_multiple_docs(prices, 'cc_price', self.indices['token_prices'])

  def _iterate_prices(self):
    return self.client.iterate(self.indices['token_prices'], 'cc_price', 'fsym:*')
