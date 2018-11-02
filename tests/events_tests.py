import unittest
import web3
from web3 import Web3, HTTPProvider
from tests.test_utils import TestClickhouse
from operations.events import Events
import httpretty
from hexbytes import HexBytes
from unittest.mock import MagicMock, Mock, call
from tests.test_utils import mockify
import json

class EventsTestCase(unittest.TestCase):
  maxDiff = None
  def setUp(self):
    self.client = TestClickhouse()
    self.indices = {
      "block": TEST_BLOCKS_INDEX,
      "block_flag": TEST_BLOCKS_TRACES_EXTRACTED_INDEX,
      "event": TEST_EVENTS_INDEX
    }
    self.client.prepare_indices(self.indices)
    self.parity_hosts = [(None, None, "http://localhost:8545")]
    self.events = Events(self.indices, parity_hosts=self.parity_hosts)

  def _get_test_event(self):
    return {
      'address': '0x0F5D2fB29fb7d3CFeE444a200298f468908cC942',
      'logIndex': 0,
      'blockNumber': 4500000,
      'blockHash': HexBytes('0x43340a6d232532c328211d8a8c0fa84af658dbff1f4906ab7a7d4e41f82fe3a3'),
      'transactionHash': HexBytes('0x93159c656e7a4c11624b7935eb507125cf82f1aae9694fbacf5470bed7d84772'),
      'transactionIndex': 2,
      'type': 'mined',
      'transactionLogIndex': '0x0',
      'data': '0x000000000000000000000000000000000000000000000b3cb19896ad16d0c000',
      'topics': [HexBytes('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'), HexBytes('0x0000000000000000000000004d468cf47eb6df39618dc9450be4b56a70a520c1'), HexBytes('0x000000000000000000000000915c0d974fef3593444028a232fda420fd6e9d1a')]
    }

  def test_iterate_block_ranges(self):
    test_range_size = 10

    self.client.bulk_index(index=TEST_BLOCKS_INDEX, docs=[{
      "id": i,
      "number": i
    } for i in range(3 * test_range_size)])

    self.client.bulk_index(index=TEST_BLOCKS_TRACES_EXTRACTED_INDEX, docs=[{
      "id": i + 10,
      "name": "events_extracted",
      "value": 1
    } for i in range(test_range_size)])

    self.client.bulk_index(index=TEST_BLOCKS_TRACES_EXTRACTED_INDEX, docs=[{
      "id": i + 20,
      "name": "other_flag",
      "value": 1
    } for i in range(test_range_size)])

    self.client.bulk_index(index=TEST_BLOCKS_TRACES_EXTRACTED_INDEX, docs=[{
      "id": i,
      "name": "events_extracted",
      "value": 1
    } for i in range(test_range_size)] + [{
      "id": i,
      "name": "events_extracted"
    }  for i in range(test_range_size)])

    result = [r for r in self.events._iterate_block_ranges(test_range_size)]
    self.assertCountEqual(result, [(0, 10), (20, 30)])
    pass

  @httpretty.activate
  def test_get_events(self):
    test_event = self._get_test_event()
    test_json_event = test_event.copy()
    test_json_event["blockHash"] = test_event["blockHash"].hex()
    test_json_event["blockNumber"] = hex(test_event["blockNumber"])
    test_json_event["logIndex"] = hex(test_event["logIndex"])
    test_json_event["transactionIndex"] = hex(test_event["transactionIndex"])
    test_json_event["transactionHash"] = test_event["transactionHash"].hex()
    test_json_event["topics"] = [topic.hex() for topic in test_event["topics"]]
    test_range = (10, 20)
    response_body = json.dumps({
      "id": 1,
      "jsonrpc": "2.0",
      "result": [test_json_event]
    })
    httpretty.register_uri(
      httpretty.POST,
      "http://localhost:8545/",
      body=response_body
    )
    received_events = self.events._get_events(test_range)
    self.assertCountEqual(received_events, [test_event])

  def test_get_events_use_range(self):
    test_range = (10, 20)
    self.events.web3.eth.filter = MagicMock(return_value=self)
    self.get_all_entries = MagicMock()
    self.events._get_events(test_range)
    self.events.web3.eth.filter.assert_any_call({"fromBlock": test_range[0], "toBlock": test_range[1] - 1})

  def test_process_event(self):
    test_event = self._get_test_event()
    test_processed_event = test_event.copy()
    test_processed_event["transactionLogIndex"] = int(test_event["transactionLogIndex"], 0)
    test_processed_event["id"] = "{}.{}".format(test_event['transactionHash'].hex(), test_processed_event["transactionLogIndex"])
    test_processed_event["address"] = test_event["address"].lower()
    test_processed_event["blockHash"] = test_event["blockHash"].hex()
    test_processed_event["transactionHash"] = test_event["transactionHash"].hex()
    test_processed_event["topics"] = [topic.hex() for topic in test_event["topics"]]

    print(test_processed_event)
    processed_event = self.events._process_event(test_event)
    self.assertSequenceEqual(processed_event, test_processed_event)

  def test_save_events(self):
    test_events = [{
      "id": i
    } for i in range(0, 10)]
    test_processed_events = [{
      "id": i,
      "type": "processed"
    } for i in range(0, 10)]
    self.events._process_event = MagicMock(side_effect=test_processed_events)

    self.events._save_events(test_events)

    for event in test_events:
      self.events._process_event.assert_any_call(event)

    saved_events = self.client.search(index=TEST_EVENTS_INDEX, fields=["id", "type"], query="WHERE type = 'processed'")
    self.assertCountEqual([event["_id"] for event in saved_events], [event["id"] for event in test_processed_events])

  def test_save_processed_blocks(self):
    test_ranges = [(0, 10), (20, 30)]
    self.events._save_processed_blocks(test_ranges)
    flags = self.client.search(index=TEST_BLOCKS_TRACES_EXTRACTED_INDEX, fields=[], query="WHERE name = 'events_extracted' AND value IS NOT NULL", size=1000)
    test_flags = [str(i) for i in range(0, 10)] + [str(i) for i in range(20, 30)]
    self.assertCountEqual([flag["_id"] for flag in flags], test_flags)

  def test_extract_events(self):
    test_ranges = [(0, 10), (20, 30)]
    test_parity_events = [
      [{'id': i, 'blockNumber': i} for i in range(10)],
      [{'id': i, 'blockNumber': i + 10} for i in range(10)]
    ]
    mockify(self.events, {
      "_iterate_block_ranges": MagicMock(return_value=test_ranges),
      '_get_events': MagicMock(side_effect=test_parity_events),
    }, 'extract_events')
    process = Mock(
      iterate_blocks=self.events._iterate_block_ranges,
      get_events=self.events._get_events,
      save_events=self.events._save_events
    )

    self.events.extract_events()

    event_calls = []
    for i, events in enumerate(test_parity_events):
      event_calls += [call.get_events(test_ranges[i]), call.save_events(events)]
    process.assert_has_calls([
      call.iterate_blocks()
    ] + event_calls)

TEST_BLOCKS_INDEX = "test_ethereum_block"
TEST_BLOCKS_TRACES_EXTRACTED_INDEX = "test_ethereum_block_flag"
TEST_EVENTS_INDEX = "test_ethereum_event"