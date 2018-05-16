from pyelasticsearch import ElasticSearch
from tqdm import *
from pyelasticsearch.exceptions import ElasticHttpError

STRING_PROPERTIES = [
  "from", "hash", 
  "blockTimestamp", "callType", 
  "gas", "gasUsed", "output"
]
OBJECT_PROPERTIES = ["decoded_input", "traceAddress"]
NUMBER_OF_JOBS = 10

class CustomElasticSearch(ElasticSearch):
  def __init__(self, *args, **kwargs):
    kwargs["timeout"] = 600
    super().__init__(*args, **kwargs)
  
  def make_range_query(self, field, range_tuple, *args):
    if len(args):
      requests = [self.make_range_query(field, range_tuple) for range_tuple in [range_tuple] + list(args)]
      result_request = " OR ".join(requests)
      return "({})".format(result_request)
    else:
      bottom_line = range_tuple[0]
      upper_bound = range_tuple[1]
      if (bottom_line is not None) and (upper_bound is not None):
        return "{}:[{} TO {}]".format(field, bottom_line, upper_bound - 1)
      elif (bottom_line is not None):
        return "{}:[{} TO *]".format(field, bottom_line)
      elif (upper_bound is not None):
        return "{}:[* TO {}]".format(field, upper_bound - 1)
      else:
        return "{}:[* TO *]".format(field)

  def update_by_query(client, index, doc_type, query, script):
    body = {'script': {'inline': script}}
    parameters = {'conflicts': 'proceed', 'refresh': True}
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

  def iterate(client, index, doc_type, query, per=NUMBER_OF_JOBS):
    items_count = client._count_by_object_or_string_query(query, index=index, doc_type=doc_type)['count']
    pages = round(items_count / per + 0.4999)
    scroll_id = None
    for page in tqdm(range(pages)):
      if not scroll_id:
        pagination_parameters = {'scroll': '60m', 'size': per}
        pagination_body = {}
        if type(query) is str:
          pagination_parameters['q'] = query
        else:
          pagination_body['query'] = query
        response = client.send_request('GET', [index, doc_type, '_search'], pagination_body, pagination_parameters)
        scroll_id = response['_scroll_id']
        page_items = response['hits']['hits']
      else:
        page_items = client.send_request('POST', ['_search', 'scroll'], {'scroll': '60m', 'scroll_id': scroll_id}, {})['hits']['hits']
      yield page_items

  def _set_string_properties_mapping(self, index, doc_type):
    mapping = {}
    for property in STRING_PROPERTIES:
      mapping[property] = {"type": "string", "index": "no"}
    self.put_mapping(index, doc_type, {'properties': mapping})

  def _set_object_properties_mapping(self, index, doc_type):
    mapping = {}
    for property in OBJECT_PROPERTIES:
      mapping[property] = {"type": "object", "enabled": False}
    self.put_mapping(index, doc_type, {'properties': mapping})

  def _disable_all_field(self, index, doc_type):
    self.put_mapping(index, doc_type, {'_all': {"enabled": False}})

  def _create_index_with_best_compression(self, index):
    self.send_request('PUT', [index], {
      "settings": {
        "index.codec": "best_compression",
      }
    }, {})

  def _set_max_result_size(self, index, max_result_window=100000):
    self.update_settings(index, {
      "index.max_result_window": max_result_window
    })

  def _index_exists(self, index):
    try:
      self.refresh(index)
      return True
    except ElasticHttpError as e:
      return False

  def prepare_fast_index(self, index, doc_type):
    if not self._index_exists(index):
      self._create_index_with_best_compression(index)
      self._set_max_result_size(index)
      self._disable_all_field(index, doc_type)
      self._set_object_properties_mapping(index, doc_type)
      self._set_string_properties_mapping(index, doc_type)
