from custom_elastic_search import CustomElasticSearch

STRING_PROPERTIES = ["from", "hash"]
OBJECT_PROPERTIES = ["decoded_input", "trace"]

class Mappings:
  def __init__(self, elasticsearch_index, elasticsearch_host="http://localhost:9200"):
    self.index = elasticsearch_index
    self.client = CustomElasticSearch(elasticsearch_host)
  
  def _set_string_properties_mapping(self):
    mapping = {}
    for property in STRING_PROPERTIES:
      mapping[property] = {"type": "text", "index": False}
    self.client.put_mapping(self.index, 'tx', {'properties': mapping})

  def _set_object_properties_mapping(self):
    mapping = {}
    for property in OBJECT_PROPERTIES:
      mapping[property] = {"type": "object", "enabled": False}
    self.client.put_mapping(self.index, 'tx', {'properties': mapping})

  def _disable_all_field(self):
    self.client.put_mapping(self.index, '_default_', {'_all': {"enabled": False}})

  def reduce_index_size(self):
    self._disable_all_field()
    self.client.index(index=self.index, doc_type='tx', doc={'test': 1}, id='start')
    try:
      self._set_object_properties_mapping()
      self._set_string_properties_mapping()
    except:
      pass
    self.client.delete(index=self.index, doc_type='tx', id='start')