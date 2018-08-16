import unittest
from blocks import Blocks
from tests.test_utils import TestElasticSearch, mockify
import httpretty
import json
from unittest.mock import MagicMock, Mock, call
from datetime import datetime

class BlocksTestCase(unittest.TestCase):
  def setUp(self):
    self.blocks = Blocks({"block": TEST_BLOCKS_INDEX}, parity_host="http://localhost:8545")
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_BLOCKS_INDEX)

  @httpretty.activate
  def test_get_max_parity_block(self):
    test_block = 100
    httpretty.register_uri(
      httpretty.POST,
      "http://localhost:8545/",
      body=json.dumps({
        "id": 1,
        "jsonrpc": "2.0",
        "result": hex(test_block)
      })
    )

    max_block = self.blocks._get_max_parity_block()

    assert httpretty.last_request().headers["Content-Type"] == "application/json"
    request = json.loads(httpretty.last_request().body.decode("utf-8"))
    assert request["id"]
    assert request["jsonrpc"] == "2.0"
    assert request["method"] == "eth_blockNumber"
    assert not len(request["params"])
    assert max_block == test_block

  def test_get_max_elasticsearch_block(self):
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 1
    }, refresh=True)
    self.client.index(index=TEST_BLOCKS_INDEX, doc_type="b", doc={
      "number": 2
    }, refresh=True)
    max_block = self.blocks._get_max_elasticsearch_block()
    assert max_block == 2

  def test_get_max_elasticsearch_block_empty_index(self):
    max_block = self.blocks._get_max_elasticsearch_block()
    assert max_block == 0

  def test_create_blocks_by_range(self):
    mockify(self.blocks, {}, "_create_blocks")
    self.blocks._create_blocks(1, 3)
    blocks = self.client.search(index=TEST_BLOCKS_INDEX, doc_type="b", query="*")['hits']['hits']
    blocks = [block["_source"]["number"] for block in blocks]
    self.assertCountEqual(blocks, [1, 2, 3])

  def test_create_unique_blocks(self):
    mockify(self.blocks, {}, "_create_blocks")
    self.blocks._create_blocks(1, 1)
    self.blocks._create_blocks(1, 1)
    blocks_number = self.client.count(index=TEST_BLOCKS_INDEX, doc_type="b", query="*")['count']
    assert blocks_number == 1

  def test_create_blocks_with_timestamp(self):
    mockify(self.blocks, {
      "_extract_block_timestamp": MagicMock(side_effect=[1, 2, 3])
    }, "_create_blocks")

    self.blocks._create_blocks(1, 3)

    blocks = self.client.search(index=TEST_BLOCKS_INDEX, doc_type="b", query="_exists_:timestamp")['hits']['hits']
    for block in [1, 2, 3]:
      self.blocks._extract_block_timestamp.assert_any_call(block)
    blocks = [block["_source"]["timestamp"] for block in blocks]
    self.assertCountEqual(blocks, [1, 2, 3])

  def test_extract_block_timestamp(self):
    # https://etherscan.io/block/10
    block_time = self.blocks._extract_block_timestamp(10)
    print(block_time)
    assert block_time < datetime(2015, 7, 31)
    assert block_time > datetime(2015, 7, 30)

  def test_create_no_blocks(self):
    self.blocks._create_blocks(1, 0)
    assert True

  def test_create_blocks(self):
    test_max_parity_block = 10
    test_max_elasticsearch_block = 1

    mockify(self.blocks, {
      "_get_max_parity_block": MagicMock(return_value=test_max_parity_block),
      "_get_max_elasticsearch_block": MagicMock(return_value=test_max_elasticsearch_block)
    }, "create_blocks")
    process = Mock(
      max_parity_block=self.blocks._get_max_parity_block,
      max_elasticsearch_block=self.blocks._get_max_elasticsearch_block,
      create_blocks=self.blocks._create_blocks
    )

    self.blocks.create_blocks()
    process.assert_has_calls([
      call.max_parity_block(),
      call.max_elasticsearch_block(),
      call.create_blocks(test_max_elasticsearch_block + 1, test_max_parity_block)
    ])

TEST_BLOCKS_INDEX = "test-ethereum-blocks"