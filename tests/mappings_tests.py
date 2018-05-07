import unittest
from mappings import Mappings
from test_utils import TestElasticSearch
import subprocess
import json
import matplotlib.pyplot as plt
from tqdm import *

CURRENT_ELASTICSEARCH_SIZE = 290659165119

class MappingsTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_INDEX)
    self.mappings = Mappings(TEST_INDEX)

  def _get_elasticsearch_size(self):
    result = subprocess.run(["du", "-sb", "/var/lib/elasticsearch"], stdout=subprocess.PIPE)
    return int(result.stdout.split()[0])

  def _add_records(self, doc, number=10000):
    docs = [{**doc, **{"id": i + 1}} for i in range(0, number)]
    self.client.bulk_index(index=TEST_INDEX, doc_type='tx', docs=docs, refresh=True)

  def test_final_index_size(self):
    transaction = TEST_TRANSACTION

    self.client.recreate_index(TEST_INDEX)
    self._add_records(transaction)
    size_before = self._get_elasticsearch_size()

    self.client.recreate_index(TEST_INDEX)
    self.mappings.reduce_index_size()
    self._add_records(transaction)
    size_after = self._get_elasticsearch_size()    

    compression = size_after / size_before
    print("Compression: {:.1%}".format(compression))
    print("Current size: {:.1f}".format(CURRENT_ELASTICSEARCH_SIZE / (1024 ** 3)))
    print("Compressed size: {:.1f}".format(compression * CURRENT_ELASTICSEARCH_SIZE / (1024 ** 3)))
    assert size_after < size_before

  def test_index_size_depending_on_records_number(self):
    x = list(range(1, 10000, 1000))
    y = []
    for i in tqdm(x):
      self.client.recreate_index(TEST_INDEX)
      self._add_records(TEST_TRANSACTION, i)
      y.append(self._get_elasticsearch_size())
    plt.plot(x, y)
    plt.show()


  def test_non_empty_index(self):
    self._add_records(TEST_TRANSACTION)
    size_before = self._get_elasticsearch_size()
    self.mappings.reduce_index_size()
    size_after = self._get_elasticsearch_size()

TEST_INDEX = 'test-ethereum-transactions'
TEST_TRANSACTION = json.loads('{"blockNumber":872857,"blockTimestamp":"2016-01-19T18:50:06","from":"0x2ef08b6fd5616ef3771406f62f2e1615db9223dc","hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e6","input":"0x3f887fad000000000000000000000001878ace426dbfc40cf00c7479a1a544c3229531b700000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000015af1d78b58c400000000000000000000000000000000000000000000000000000000000000000000","to":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","to_contract":true,"trace":[{"action":{"callType":"call","from":"0x2ef08b6fd5616ef3771406f62f2e1615db9223dc","gas":"0x2d6a68","input":"0x3f887fad000000000000000000000001878ace426dbfc40cf00c7479a1a544c3229531b700000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000015af1d78b58c400000000000000000000000000000000000000000000000000000000000000000000","to":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","value":"0x9d6457c2e2e29ecd"},"class":0,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e60","result":{"gasUsed":"0xe53a","output":"0x"},"subtraces":10,"traceAddress":[],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2d0381","input":"0x24d4e90a0000000000000000000000000000000000000000000000030000000000000000","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e61","result":{"gasUsed":"0x9e0","output":"0x000000000000000000000000000000000000000000000001193ea7aad0311384"},"subtraces":0,"traceAddress":[0],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2ced85","input":"0x4b09ebb20000000000000000000000000000000000000000000000005c878eb6eae51aa0","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e62","result":{"gasUsed":"0x2da","output":"0x0000000000000000000000000000000000000000000000016f76596861d8b7f9"},"subtraces":0,"traceAddress":[1],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2ce83c","input":"0x4b09ebb20000000000000000000000000000000000000000000000000e0feec88a68d940","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e63","result":{"gasUsed":"0x2da","output":"0x0000000000000000000000000000000000000000000000010e74a452d439ed42"},"subtraces":0,"traceAddress":[2],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2ce2f3","input":"0x4b09ebb20000000000000000000000000000000000000000000000000000000000000000","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e64","result":{"gasUsed":"0x2da","output":"0x00000000000000000000000000000000000000000000000100000016aee6e8ef"},"subtraces":0,"traceAddress":[3],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2cde62","input":"0x24d4e90a0000000000000000000000000000000000000000000000037deafdd1e4f98e2a","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e65","result":{"gasUsed":"0x9e0","output":"0x000000000000000000000000000000000000000000000001401c9bc97658c7c2"},"subtraces":0,"traceAddress":[4],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2cbcee","input":"0x4b09ebb2000000000000000000000000000000000000000000000000a2d738a19ef158e0","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e66","result":{"gasUsed":"0x2da","output":"0x000000000000000000000000000000000000000000000001e39ac8cf61d61e8f"},"subtraces":0,"traceAddress":[5],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2cb7a5","input":"0x4b09ebb20000000000000000000000000000000000000000000000000e0feec88a68d940","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e67","result":{"gasUsed":"0x2da","output":"0x0000000000000000000000000000000000000000000000010e74a452d439ed42"},"subtraces":0,"traceAddress":[6],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2cb25c","input":"0x4b09ebb20000000000000000000000000000000000000000000000000000000000000000","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e68","result":{"gasUsed":"0x2da","output":"0x00000000000000000000000000000000000000000000000100000016aee6e8ef"},"subtraces":0,"traceAddress":[7],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x2cadcb","input":"0x24d4e90a000000000000000000000000000000000000000000000003f20f6d38e4f6f4c0","to":"0x258c09146b7a28dde8d3e230030e27643f91115f","value":"0x0"},"class":3,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e69","result":{"gasUsed":"0x9e0","output":"0x0000000000000000000000000000000000000000000000015f61ea75151f9cf5"},"subtraces":0,"traceAddress":[8],"type":"call"},{"action":{"callType":"call","from":"0x2935aa0a2d2fbb791622c29eb1c117b65b7a9085","gas":"0x0","input":"0x","to":"0x2ef08b6fd5616ef3771406f62f2e1615db9223dc","value":"0x0"},"class":1,"hash":"0xa985ae981384463d010b948a1ca9835dde81d7bc0a55c376e0d07f96b66f28e610","result":{"gasUsed":"0x0","output":"0x"},"subtraces":0,"traceAddress":[9],"type":"call"}],"value":11.341286256167527}')