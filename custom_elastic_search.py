from pyelasticsearch import ElasticSearch
from tqdm import *

NUMBER_OF_JOBS = 10

class CustomElasticSearch(ElasticSearch):
  def update_by_query(client, index, doc_type, query, script):
    body = {'script': {'inline': script}}
    parameters = {'refresh': True}
    if type(query) is dict:
      body['query'] = query
    else:
      parameters['q'] = query
    client.send_request('POST', [index, doc_type, '_update_by_query'], body, parameters)

  def _count_by_object_or_string_query(client, query, index, doc_type):
    count_body = ''
    count_parameters = {}
    if type(query) is str:
      count_parameters['q'] = query
    else:
      count_body = {
        'query': query
      }
    return client.send_request('GET', [index, doc_type, '_count'], count_body, count_parameters)

  def iterate(client, index, doc_type, query, per=NUMBER_OF_JOBS, paginate=False, scrolling=True):
    items_count = client._count_by_object_or_string_query(query, index=index, doc_type=doc_type)['count']
    pages = round(items_count / per + 0.4999)
    scroll_id = None
    for page in tqdm(range(pages)):
      if paginate:
        if not scroll_id:
          pagination_parameters = {'scroll': '1m', 'size': per}
          pagination_body = {}
          if type(query) is str:
            pagination_parameters['q'] = query
          else:
            pagination_body['query'] = query
          response = client.send_request('GET', [index, doc_type, '_search'], pagination_body, pagination_parameters)
          scroll_id = response['_scroll_id']
          page_items = response['hits']['hits']
        else:
          page_items = client.send_request('POST', ['_search', 'scroll'], {'scroll': '1m', 'scroll_id': scroll_id}, {})['hits']['hits']
      else:
        page_items = client.search(query, index=index, doc_type=doc_type, size=per)['hits']['hits']
      yield page_items
