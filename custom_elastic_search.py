from pyelasticsearch import ElasticSearch
from tqdm import *
from pyelasticsearch.exceptions import ElasticHttpError

STRING_PROPERTIES = {
  "tx": ["from", "hash", "blockTimestamp", "creates", "to"],
  "itx": [
    "from", "hash",
    "blockTimestamp", "callType",
    "gas", "gasUsed",
    "callType", "blockHash", "transactionHash",
    "refundAddress", "to",
    "type", "address", "balance", "blockNumber"
  ]
}

OBJECT_PROPERTIES = {
  "tx": ["decoded_input"],
  "itx": ["decoded_input", "traceAddress"]
}

TEXT_PROPERTIES = {
  "tx": ["input"],
  "itx": ["code", "input", "init", "error", "output"]
}

NUMBER_OF_JOBS = 1000

class CustomElasticSearch(ElasticSearch):
  def __init__(self, *args, **kwargs):
    kwargs["timeout"] = 600
    super().__init__(*args, **kwargs)

  def make_range_query(self, field, range_tuple, *args):
    """
    Create ElasticSearch request to get all documents with specified field in specified range

    Parameters
    ----------
    field : string
        Contracts info in ElasticSearch JSON format, i.e.
        {"_id": TRANSACTION_ID, "_source": {"document": "fields"}}
    range_tuple : int
        Tuple in a format of (start_block, end_block)
    *args : list
        Other tuples, or empty

    Returns
    -------
    str
        ElasticSearch query in a form of:
        (field:[1 TO 2] OR field:[4 TO *])
    """
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
    """
    Update ElasticSearch records by specified query with specified script

    Parameters
    ----------
    index : str
        ElasticSearch index
    doc_type : str
        Document type
    query : dict or str
        ElasticSearch query
    script : str
        Script for update operation
    """
    body = {'script': {'inline': script}}
    parameters = {'conflicts': 'proceed', 'refresh': True}
    if type(query) is dict:
      body['query'] = query
    else:
      parameters['q'] = query
    client.send_request('POST', [index, doc_type, '_update_by_query'], body, parameters)

  def _count_by_object_or_string_query(client, query, index, doc_type):
    """
    Count objects in ElasticSearch by specified query

    Parameters
    ----------
    query : dict or str
        ElasticSearch query
    index : str
        ElasticSearch index
    doc_type : str
        Document type

    Returns
    -------
    int
        Number of objects in ElasticSearch
    """
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
    """
    Iterate through elasticsearch records

    Will return a chunk of records each time

    Parameters
    ----------
    index : str
        ElasticSearch index
    doc_type : str
        Document type
    query : dict or str
        ElasticSearch query
    per : int
        Max length of chunk

    Returns
    -------
    generator
        Generator that returns chunks with records by specified query
    """
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

  def _set_mapping(self, index, doc_type, properties, type):
    """
    Set specified type for each specified property in ElasticSearch index

    Parameters
    ----------
    index : str
        ElasticSearch index
    doc_type : str
        Document type
    properties : list
        List of properties
    type : dict
        Type of properties
    """
    mapping = {}
    for property in properties:
      mapping[property] = type
    self.put_mapping(index, doc_type, {'properties': mapping})

  def _set_string_properties_mapping(self, index, doc_type):
    """
    Set string type for specified document type properties

    Parameters
    ----------
    index : str
        ElasticSearch index
    doc_type : str
        Document type
    """
    self._set_mapping(
      index, doc_type,
      STRING_PROPERTIES[doc_type],
      {"type": "keyword"}
    )

  def _set_object_properties_mapping(self, index, doc_type):
    """
    Set object type for specified document type properties,
    and removes object properties indexing

    Parameters
    ----------
    index : str
        ElasticSearch index
    doc_type : str
        Document type
    """
    self._set_mapping(
      index, doc_type,
      OBJECT_PROPERTIES[doc_type],
      {"type": "object", "enabled": False}
    )

  def _set_text_properties_mapping(self, index, doc_type):
    """
    Set text type for specified document type properties,
    and sets keyword size = 10

    Parameters
    ----------
    index : str
        ElasticSearch index
    doc_type : str
        Document type
    """
    self._set_mapping(
      index, doc_type,
      TEXT_PROPERTIES[doc_type],
      {
        "type": "text",
        "index": False,
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 10
          }
        }
      }
    )

  def _disable_all_field(self, index, doc_type):
    """
    Disable _all field

    Parameters
    ----------
    index : str
        ElasticSearch index
    doc_type : str
        Document type
    """
    self.put_mapping(index, doc_type, {'_all': {"enabled": False}})

  def _create_index_with_best_compression(self, index):
    """
    Create index with best compression parameter

    Parameters
    ----------
    index : str
        ElasticSearch index
    """
    self.send_request('PUT', [index], {
      "settings": {
        "index.codec": "best_compression",
      }
    }, {})

  def _set_max_result_size(self, index, max_result_window=100000):
    """
    Create max result size window for an index

    Parameters
    ----------
    index : str
        ElasticSearch index
    max_result_window : int
        Size of window
    """
    self.update_settings(index, {
      "index.max_result_window": max_result_window
    })

  def _index_exists(self, index):
    """
    Check if index exists

    Parameters
    ----------
    index : str
        ElasticSearch index
    """
    try:
      self.refresh(index)
      return True
    except ElasticHttpError as e:
      return False

  def prepare_fast_index(self, index, doc_type):
    """
    Prepare specified index:
    - Use best compression
    - Set large result size
    - Disable _all field
    - Remove indexing on object fields
    - Use string type for text fields wherever possible
    - Use text type with small keyword size for long text

    Parameters
    ----------
    index : str
        ElasticSearch index
    doc_type : str
        Document type
    """
    if not self._index_exists(index):
      self._create_index_with_best_compression(index)
      self._set_max_result_size(index)
      self._disable_all_field(index, doc_type)
      self._set_object_properties_mapping(index, doc_type)
      self._set_string_properties_mapping(index, doc_type)
      self._set_text_properties_mapping(index, doc_type)
