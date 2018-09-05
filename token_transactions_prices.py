from config import INDICES
from pyelasticsearch import ElasticSearch, bulk_chunks
from tqdm import tqdm
from web3 import Web3, HTTPProvider
import numpy as np

NUMBER_OF_JOBS = 200
PRICE_FIELDS = {
  'USD': ["USD", "USD_cmc"],
  'ETH': ["ETH"]
}
TRANSACTION_FIELD = {
  'USD': 'usd_value',
  'ETH': 'eth_value'
}

class TokenTransactionsPrices:
  '''
  Add ETH and USD price to token transfers

  Parameters
  ----------
  elasticsearch_host: str
    Elasticsearch url
  indices: dict
    Dictionary containing exisiting Elasticsearch indices
  '''
  def __init__(self, elastic_host='http://localhost:9200', indices=INDICES):
    self.client = ElasticSearch(elastic_host)
    self.indices = indices
    self.start_block = 4000000
    self.end_block = 7000000
    self.w3 = Web3(HTTPProvider('http://localhost:8550'))

  def _count_by_object_or_string_query(self, query, index, doc_type):
    '''
    Count number of documents that match the query

    Parameters
    ----------
    query: dict or str
      Query to Elasticsearch
    index: str
      Elasticsearch index
    doc_type: str
      Elasticsearch document type

    Returns
    -------
    dict
      Count query result
    '''
    if "sort" in query:
      del query['sort']
    count_parameters = {}
    count_body = query
    return self.client.send_request('GET', [index, doc_type, '_count'], count_body, count_parameters)

  def _iterate(self, index, doc_type, query, per=NUMBER_OF_JOBS):
    '''
    Iterate over documents that match query

    Parameters
    ----------
    index: str
      Elasticsearch index
    doc_type: str
      Elasticsearch document type
    query: dict or str
      Query to Elasticsearch
    per: int
      Number of documents queried in one request
    '''
    items_count = self._count_by_object_or_string_query(query, index=index, doc_type=doc_type)['count']
    pages = round(items_count / per + 0.4999)
    scroll_id = None
    for page in tqdm(range(pages)):
      if not scroll_id:
        pagination_parameters = {'scroll': '60m', 'size': per}
        pagination_body = {}
        pagination_body = query
        response = self.client.send_request('GET', [index, doc_type, '_search'], pagination_body, pagination_parameters)
        scroll_id = response['_scroll_id']
        page_items = response['hits']['hits']
      else:
        page_items = self.client.send_request('POST', ['_search', 'scroll'], {'scroll': '60m', 'scroll_id': scroll_id}, {})['hits']['hits']
      yield page_items

  def _get_last_day(self):
    '''
    Get timestamp of last price that has marketCap Field

    Returns
    -------
    string
      Timestamp of last available date
    '''
    query = {
      'query': {
        'exists': {'field': 'marketCap'}
      },
      'sort': {
        'timestamp': {'order': 'desc'}
      },
      'size': 1
    }
    res = self.client.send_request('GET', [self.indices['token_price'], 'price', '_search'], query, {})
    return res['hits']['hits'][0]['_source']['timestamp']

  def _get_top_syms(self, timestamp):
    '''
    Extract top-100 tokens by market capitalization in specified date

    Parameters
    ----------
    timestamp: str
      Date

    Returns
    -------
    list
      List of top-100 token symbols
    '''
    query = {
      'query': {
        'term': {'timestamp': timestamp},
      },
      'sort': {
        'marketCap': {'order': 'desc'}
      },
      'size': 100
    }
    res = self.client.send_request('GET', [self.indices['token_price'], 'price', '_search'], query, {})['hits']['hits']
    symbols = [price['_source']['token'] for price in res]
    return symbols

  def _get_top_addr(self, symbols):
    '''
    Extract addresses of top tokens

    Parameters
    ----------
    symbols: list
      Top token symbols

    Returns
    -------
    tuple
      Tupple that contain top token addresses and dict with mapping from address to symbol
    '''
    query = {
      'query': {
        'terms': {'cc_sym.keyword': symbols}
      }
    }
    res = self.client.send_request('GET', [self.indices['contract'], 'contract', '_search'], query, {})['hits']['hits']
    addresses = [contract['_source']['address'] for contract in res]
    syms_map = {contract['_source']['address']: contract['_source']['cc_sym']
      for contract in res if 'cc_sym' in contract['_source'].keys()}
    return (addresses, syms_map)

  def _iterate_top_tokens_txs(self, addresses, currency):
    '''
    Iterate over top tokens transactions

    Parameters
    ----------
    addresses: list
      Top token addresses
    currency: str
      USD or ETH; currency in which transfer price will be extracted

    Returns
    -------
    generator
      Generator that iterates over top tokens txs
    '''
    query = {
      'query': {
        'bool': {
          'must': [
            {'range': {'block_id': {'gte': self.start_block, 'lt': self.end_block}}},
            {'terms': {'token': addresses}}
          ],
          'must_not': [
            {'exists': {'field': TRANSACTION_FIELD[currency]}}
          ]
        }
      },
      'sort': {
        'block_id': {'order': 'desc'}
      }
    }
    return self._iterate(self.indices['token_tx'], 'tx', query)

  def _get_block_tss(self):
    '''
    Extract block timestamps

    Method used to add timestamp field to token transactions

    Returns
    -------
    dict
      Blocks timestamps dict
    '''
    blocks = self._iterate(index=self.indices["block"], doc_type='b', query={"query": {"range": {"number": {"gte": 0}}}})
    timestamps = {}
    for chunk in blocks:
      for block in chunk:
        timestamps[block["_source"]["number"]] = block["_source"]["timestamp"].split("T")[0]
    return timestamps

  def _get_prices_by_dates(self, dates, symbols, price_fields):
    '''
    Construct price ids from dates and symbols and download prices from Elasticsearch by their exact ids

    Parameters
    ----------
    dates: list
      Price dates
    symbols: list
      Token symbols
    price_fields: list
      List of currencies that will be used to extract prices
    Returns
    -------
    list
      List of token prices
    '''
    ids = []
    for symbol in symbols:
      for date in dates:
        id_ = symbol + '_' + date
        ids.append(id_)
    query = {
      'ids': ids
    }
    res = self.client.send_request('GET', [self.indices['token_price'], 'price', '_mget'], query, {})['docs']
    prices = {}
    market_capitalization = {}
    for price in res:
      if price['found'] == False:
        continue
      for field in price_fields:
        if (field in price['_source'].keys()) and (price["_source"][field] is not None):
          prices[price['_source']['token'] + '_' + price['_source']['timestamp']] = price['_source'][field]
          market_capitalization[price['_source']['token'] + '_' + price['_source']['timestamp']] = price['_source'].get("marketCap", 0)
          break;
    return prices, market_capitalization

  def _get_exchange_price(self, value, price):
    '''
    Convert value from one currency to another

    Parameters
    ----------
    value: float
      Transfer value
    price: float
      Token price

    Returns
    -------
    float:
      Converted value
    '''
    return float('{:0.10f}'.format(value * price))

  def _construct_bulk_update_ops(self, docs):
    '''
    Iterate over docs and create document-updating operations used in bulk update

    Parameters
    ----------
      docs: list
        List of dictionaries with new data
    '''
    for doc in docs:
      yield self.client.update_op(doc['doc'], id=doc['id'])

  def _update_multiple_docs(self, docs, doc_type, index_name):
    '''
    Update multiple documents simultaneously

    Parameters
    ----------
    docs: list
      List of dictionaries with new data
    doc_type: str 
      Type of updated documents
    index_name: str
      Name of the index that contains updated documents
    '''
    for chunk in bulk_chunks(self._construct_bulk_update_ops(docs), docs_per_chunk=1000):
      self.client.bulk(chunk, doc_type=doc_type, index=index_name, refresh=True)

  def _get_overflow(self, usd_value, capitalization):
    '''
    Check if transfer usd value is larger than token capitalization

    Parameters
    ----------
    usd_value: float
      Transfer value
    capitalization: float
      token capitalization
      
    '''
    if not capitalization:
      return 0
    else:
      return min(np.abs(usd_value / capitalization), 1)

  def extract_transactions_prices(self, currency):
    '''
    Download token prices from Elasticsearch and convert token transfer values into other currency

    This function is an entry point for extract-token-transactions-prices operation

    Parameters
    ----------
    currency: str
      Currency to convert transfer value
    '''
    last_day = self._get_last_day()
    symbols = self._get_top_syms(last_day)
    addresses, symbols_map = self._get_top_addr(symbols)
    symbols = [symbols_map[a] for a in addresses]
    results = []
    block_tss = self._get_block_tss()
    for token_txs in self._iterate_top_tokens_txs(addresses, currency):
      blocks = list(set([tx['_source']['block_id'] for tx in token_txs]))
      blocks = {block: block_tss[block] for block in blocks}
      dates = list(set([date for date in blocks.values()]))
      prices, market_capitalization = self._get_prices_by_dates(dates, symbols, PRICE_FIELDS[currency])
      update_docs = []
      for tx in token_txs:
        sym = symbols_map[tx['_source']['token']]
        timestamp = blocks[tx['_source']['block_id']]
        prices_key = sym + '_' + timestamp
        try:
          value = float(tx['_source']['value'])
          exchange_price = self._get_exchange_price(value, prices[prices_key])
        except Exception as e:
          print(tx['_source'])
          print('exception:', e)
          exchange_price = None
          value = None
        update_doc = {
          'doc': {
            TRANSACTION_FIELD[currency]: exchange_price,
            'timestamp': timestamp,
          },
          'id': tx['_id']
        }
        if currency == 'USD':
          update_doc['doc']['overflow'] = self._get_overflow(exchange_price, market_capitalization[prices_key])
        update_docs.append(update_doc)
      self._update_multiple_docs(update_docs, 'tx', self.indices['token_tx'])
