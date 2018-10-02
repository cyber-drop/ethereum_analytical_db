import unittest
from operations.blocks import ElasticSearchBlocks, ClickhouseBlocks
from operations.indices import ClickhouseIndices
from tests.test_utils import TestElasticSearch, mockify, TestClickhouse
import httpretty
import json
from unittest.mock import MagicMock, Mock, call
from datetime import datetime
from clients.custom_elastic_search import CustomElasticSearch
from clients.custom_clickhouse import CustomClickhouse

class BlocksTestCase():
  @httpretty.activate
  def test_get_max_parity_block(self):
    """Test sending request to parity to get last block"""
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
    """Test getting max block from elasticsearch"""
    docs = [{"number": 1}, {"number": 2}]
    for doc in docs:
      doc["id"] = doc["number"]
    self.client.bulk_index(index=TEST_BLOCKS_INDEX, doc_type="b", docs=docs, refresh=True)
    max_block = self.blocks._get_max_elasticsearch_block()
    assert max_block == 2

  def test_get_max_elasticsearch_block_empty_index(self):
    """Test get max block in empty ElasticSearch index"""
    max_block = self.blocks._get_max_elasticsearch_block()
    assert max_block == -1

  def test_create_blocks_by_range(self):
    """Test create blocks in ElasticSearch by range"""
    mockify(self.blocks, {}, "_create_blocks")
    self.blocks._create_blocks(1, 3)
    blocks = self.client.search(index=TEST_BLOCKS_INDEX, doc_type="b", fields=["number"])
    blocks = [block["_source"]["number"] for block in blocks]
    self.assertCountEqual(blocks, [1, 2, 3])

  def test_create_unique_blocks(self):
    mockify(self.blocks, {}, "_create_blocks")
    self.blocks._create_blocks(1, 1)
    self.blocks._create_blocks(1, 1)
    blocks_number = self.client.count(index=TEST_BLOCKS_INDEX, doc_type="b")
    print(blocks_number)
    assert blocks_number == 1

  def test_create_blocks_with_timestamp(self):
    mockify(self.blocks, {
      "_extract_block_timestamp": MagicMock(side_effect=[1, 2, 3])
    }, "_create_blocks")

    self.blocks._create_blocks(1, 3)

    # WHERE statement?! WTF!
    blocks = self.client.search(index=TEST_BLOCKS_INDEX, query=None, fields=["timestamp"])
    for block in [1, 2, 3]:
      self.blocks._extract_block_timestamp.assert_any_call(block)
    blocks = [block["_source"]["timestamp"].timestamp() for block in blocks]
    self.assertCountEqual(blocks, [1, 2, 3])

  def test_extract_block_timestamp(self):
    # https://etherscan.io/block/10
    block_time = self.blocks._extract_block_timestamp(10)
    print(block_time)
    assert block_time < datetime(2015, 7, 31)
    assert block_time > datetime(2015, 7, 30)

  def test_extract_block_timestamp_no_such_block(self):
    block_time = self.blocks._extract_block_timestamp(9999999)
    assert not block_time

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

class ElasticSearchBlocksTestCase(BlocksTestCase, unittest.TestCase):
  def setUp(self):
    self.blocks = ElasticSearchBlocks(
      {"block": TEST_BLOCKS_INDEX},
      parity_host="http://localhost:8545"
    )
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_BLOCKS_INDEX)

class ClickhouseBlocksTestCase(BlocksTestCase, unittest.TestCase):
  def setUp(self):
    self.blocks = ClickhouseBlocks(
      {"block": TEST_BLOCKS_INDEX},
      parity_host="http://localhost:8545"
    )
    self.client = TestClickhouse()
    self.client.send_sql_request("DROP TABLE IF EXISTS {}".format(TEST_BLOCKS_INDEX))
    ClickhouseIndices({"block": TEST_BLOCKS_INDEX}).prepare_indices()

TEST_BLOCKS_INDEX = "test_ethereum_blocks"