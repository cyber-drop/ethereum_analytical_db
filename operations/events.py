from clients.custom_clickhouse import CustomClickhouse
from config import EVENTS_RANGE_SIZE, INDICES, PARITY_HOSTS
from web3 import Web3, HTTPProvider

class Events:
  def __init__(self, indices=INDICES, parity_hosts=PARITY_HOSTS):
    self.client = CustomClickhouse()
    self.indices = indices
    self.web3 = Web3(HTTPProvider(parity_hosts[0][-1]))

  def _iterate_block_ranges(self, range_size=EVENTS_RANGE_SIZE):
    range_query = "distinct(toInt32(floor(number / {}))) AS range".format(range_size)
    flags_query = "ANY LEFT JOIN (SELECT id, value FROM {} FINAL WHERE name = 'events_extracted') USING id WHERE value IS NULL".format(self.indices["block_flag"])
    for ranges_chunk in self.client.iterate(index=self.indices["block"], fields=[range_query], query=flags_query, return_id=False):
      for range in ranges_chunk:
        range_bounds = (
          range["_source"]["range"] * range_size,
          (range["_source"]["range"] + 1) * range_size
        )
        yield range_bounds

  def _get_events(self, block_range):
    event_filter = self.web3.eth.filter({"fromBlock": block_range[0], "toBlock": block_range[1] - 1})
    events = event_filter.get_all_entries()
    return events

  def _save_events(self, events):
    events = [self._process_event(event) for event in events]
    if events:
      self.client.bulk_index(index=self.indices["event"], docs=events)

  def _process_event(self, event):
    processed_event = event.copy()
    processed_event["transactionLogIndex"] = int(event["transactionLogIndex"], 0)
    processed_event["id"] = "{}.{}".format(event['transactionHash'].hex(), processed_event["transactionLogIndex"])
    processed_event["address"] = event["address"].lower()
    processed_event["blockHash"] = event["blockHash"].hex()
    processed_event["transactionHash"] = event["transactionHash"].hex()
    processed_event["topics"] = [topic.hex() for topic in event["topics"]]
    return processed_event

  def _save_processed_blocks(self, block_range):
    block_flags = [{
      "id": block,
      "name": "events_extracted",
      "value": 1
    } for block in range(*block_range)]
    self.client.bulk_index(index=self.indices["block_flag"], docs=block_flags)

  def extract_events(self):
    for block_range in self._iterate_block_ranges():
      events = self._get_events(block_range)
      self._save_events(events)
      self._save_processed_blocks(block_range)