import unittest
from operations.blocks import ClickhouseBlocks
from operations.indices import ClickhouseIndices
from tests.test_utils import mockify, TestClickhouse, parity
import httpretty
import json
from unittest.mock import MagicMock, Mock, call
from datetime import datetime, timedelta
from web3 import Web3, HTTPProvider
from pprint import pprint
from config import ETHEREUM_START_DATE, TEST_PARITY_NODE

TEST_BLOCKS_INDEX = "test_ethereum_blocks"
TEST_PARITY_URL = TEST_PARITY_NODE


class ClickhouseBlocksTestCase(unittest.TestCase):
    def setUp(self):
        self.blocks = ClickhouseBlocks(
            {"block": TEST_BLOCKS_INDEX},
            parity_host=TEST_PARITY_URL
        )
        self.client = TestClickhouse()
        self.client.send_sql_request("DROP TABLE IF EXISTS {}".format(TEST_BLOCKS_INDEX))
        ClickhouseIndices({"block": TEST_BLOCKS_INDEX}).prepare_indices()

    @httpretty.activate
    def test_get_max_parity_block(self):
        """Test sending request to parity to get last block"""
        test_block = 100
        httpretty.register_uri(
            httpretty.POST,
            TEST_PARITY_URL,
            body=json.dumps({
                "id": 1,
                "jsonrpc": "2.0",
                "result": {
                    "startingBlock": hex(test_block),
                    "currentBlock": hex(test_block + 2),
                    "highestBlock": hex(test_block + 1)
                }
            })
        )

        max_block = self.blocks._get_max_parity_block()

        assert max_block == test_block + 1

    @httpretty.activate
    def test_get_max_parity_block_no_sync(self):
        test_max_block = 100
        eth = self.blocks.w3.eth
        self.blocks.w3.eth = MagicMock(syncing=False)
        self.blocks.w3.eth.getBlock = eth.getBlock
        httpretty.register_uri(
            httpretty.POST,
            TEST_PARITY_URL,
            body=json.dumps({
                "id": 1,
                "jsonrpc": "2.0",
                "result": {
                    "number": test_max_block
                }
            })
        )
        max_block = self.blocks._get_max_parity_block()
        print(max_block)

        assert max_block == test_max_block

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
        self.blocks._create_blocks(1, 5, max_blocks=3)
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
        test_datetimes = [
            datetime.today(),
            datetime.today() + timedelta(days=1),
            datetime.today() + timedelta(days=2)
        ]
        mockify(self.blocks, {
            "_extract_block_timestamp": MagicMock(side_effect=test_datetimes)
        }, "_create_blocks")

        self.blocks._create_blocks(1, 3)

        blocks = self.client.search(index=TEST_BLOCKS_INDEX, fields=["timestamp"])
        for block in [1, 2, 3]:
            self.blocks._extract_block_timestamp.assert_any_call(block)
        blocks = [block["_source"]["timestamp"].date() for block in blocks]
        self.assertCountEqual(blocks, [d.date() for d in test_datetimes])

    @parity
    def test_extract_block_timestamp(self):
        # https://etherscan.io/block/10
        block_time = self.blocks._extract_block_timestamp(10)
        print(block_time)
        assert block_time < datetime(2015, 7, 31)
        assert block_time > datetime(2015, 7, 30)

    def test_extract_start_block_timestamp(self):
        block_time = self.blocks._extract_block_timestamp(0)
        print(block_time)
        assert block_time == ETHEREUM_START_DATE
        self.client.bulk_index(index=TEST_BLOCKS_INDEX, docs=[{
            "id": "test",
            "number": 0,
            "timestamp": block_time
        }])

    @parity
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

    @parity
    def test_process(self):
        w3 = Web3(HTTPProvider(TEST_PARITY_URL))
        latest_block = w3.eth.getBlock('latest').number
        start_block = latest_block - 10
        self.client.bulk_index(docs=[{
            "id": start_block,
            "number": start_block
        }], index=TEST_BLOCKS_INDEX)
        self.blocks.create_blocks()
        blocks = self.client.search(index=TEST_BLOCKS_INDEX, fields=["number", "timestamp"])
        pprint(blocks)
        assert len(blocks) == 11
