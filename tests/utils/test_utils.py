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
      "number": 0
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 1
    }, refresh=True)
    max_block = get_max_block()
    assert max_block == 1
    assert type(max_block) == int

  def test_get_max_block_by_a_query(self):
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 0,
      "trace": True
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 1
    }, refresh=True)
    max_block = get_max_block("trace:true")
    assert max_block == 0

  def test_get_max_consistent_block(self):
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 0,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 1,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 2
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 4
    }, refresh=True)
    max_block = get_max_block()
    assert max_block == 2

  def test_get_max_consistent_block_return_min_consistent_block_if_ended(self):
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 0,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 1,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 3
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 5
    }, refresh=True)
    max_block = get_max_block(min_consistent_block=3)
    assert max_block == 3

  def test_get_max_consistent_block_ignore_inconsistency_before_min_block(self):
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 0,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 2,
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 3
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 5
    }, refresh=True)
    max_block = get_max_block(min_consistent_block=2)
    assert max_block == 3

TEST_BLOCKS_INDEX = "test-ethereum-blocks"
config.INDICES.update({
  "block": TEST_BLOCKS_INDEX
})