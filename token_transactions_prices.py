from config import INDICES
from pyelasticsearch import ElasticSearch, bulk_chunks
from tqdm import tqdm
from web3 import Web3, HTTPProvider

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
  def __init__(self, elastic_host='http://localhost:9200', indices=INDICES):
    self.client = ElasticSearch(elastic_host)
    self.indices = indices
    self.start_block = 4000000
    self.end_block = 7000000
    self.w3 = Web3(HTTPProvider('http://localhost:8550'))

  def _count_by_object_or_string_query(self, query, index, doc_type):
    if "sort" in query:
      del query['sort']
    count_parameters = {}
    count_body = query
    return self.client.send_request('GET', [index, doc_type, '_count'], count_body, count_parameters)

  def _iterate(self, index, doc_type, query, per=NUMBER_OF_JOBS):
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
    blocks = self._iterate(index=self.indices["block"], doc_type='b', query={"query": {"range": {"number": {"gte": 0}}}})
    timestamps = {}
    for chunk in blocks:
      for block in chunk:
        timestamps[block["_source"]["number"]] = block["_source"]["timestamp"].split("T")[0]
    return timestamps

  def _get_prices_by_dates(self, dates, symbols, price_fields):
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
    return float('{:0.10f}'.format(value * price))

  def _construct_bulk_update_ops(self, docs):
    for doc in docs:
      yield self.client.update_op(doc['doc'], id=doc['id'])

  def _update_multiple_docs(self, docs, doc_type, index_name):
    for chunk in bulk_chunks(self._construct_bulk_update_ops(docs), docs_per_chunk=1000):
      self.client.bulk(chunk, doc_type=doc_type, index=index_name, refresh=True)

  def _get_overflow(self, usd_value, capitalization):
    if not capitalization:
      return 0
    else:
      return min(usd_value / capitalization, 1)

  def extract_transactions_prices(self, currency):
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
