import unittest
from utils import get_max_block
from tests.test_utils import TestElasticSearch
import config

class UtilsTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_BLOCKS_INDEX)

  def test_get_max_block(self):
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 1
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 2
    }, refresh=True)
    max_block = get_max_block()
    assert max_block == 2
    assert type(max_block) == int

TEST_BLOCKS_INDEX = "test-ethereum-blocks"
config.INDICES.update({
  "block": TEST_BLOCKS_INDEX
})