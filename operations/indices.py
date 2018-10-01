from config import INDICES
from clients.custom_elastic_search import CustomElasticSearch
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

FAST_INDICES = {
  "internal_transaction": "itx"
}

class ElasticSearchIndices:
  def __init__(self, indices=INDICES):
    self.indices = indices
    self.client = CustomElasticSearch("http://localhost:9200")

  def _set_mapping(self, index, doc_type, properties, type):
    mapping = {}
    for property in properties:
      mapping[property] = type
    self.client.put_mapping(index, doc_type, {'properties': mapping})

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

  def _set_text_properties_mapping(self, index, doc_type):
    """
    Set text type for specified document type properties,
    and sets keyword size = 256

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
            "ignore_above": 256
          }
        }
      }
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
    self.client.put_mapping(index, doc_type, {'_all': {"enabled": False}})

  def _create_index_with_best_compression(self, index):
    """
    Create index with best compression parameter

    Parameters
    ----------
    index : str
        ElasticSearch index
    """
    self.client.send_request('PUT', [index], {
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
    self.client.update_settings(index, {
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
      self.client.refresh(index)
      return True
    except ElasticHttpError as e:
      return False

  def _prepare_fast_index(self, index, doc_type):
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

  def _prepare_index(self, index):
    if not self._index_exists(index):
      self.client.create_index(index)

  def prepare_indices(self):
    for index in self.indices:
      if index in FAST_INDICES:
        self._prepare_fast_index(self.indices[index], FAST_INDICES[index])
      else:
        self._prepare_index(self.indices[index])

class ClickhouseIndices:
  pass