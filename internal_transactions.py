from custom_elastic_search import CustomElasticSearch
import requests
import json
from multiprocessing import Pool
from itertools import repeat
from config import PARITY_HOSTS, GENESIS
import config
import pygtrie as trie
import utils
from pyelasticsearch import bulk_chunks

BYTES_PER_CHUNK = 1000000
NUMBER_OF_PROCESSES = 10

INPUT_TRANSACTION = 0
INTERNAL_TRANSACTION = 1
OUTPUT_TRANSACTION = 2
OTHER_TRANSACTION = 3

MAX_BLOCKS_NUMBER = 10000000

def _get_parity_url_by_block(parity_hosts, block):
  """
  Get url of a parity JSON RPC API for specified block

  Parameters
  ----------
  parity_hosts : list
      List of tuples with each parity JSON RPC url and used block range
  block : int
      Block number

  Returns
  -------
  str
      Url of parity API that will serve specified block
  """
  for bottom_line, upper_bound, url in parity_hosts:
    if ((not bottom_line) or (block >= bottom_line)) and ((not upper_bound) or (block < upper_bound)):
      return url

def _make_trace_requests(parity_hosts, blocks):
  """
  Create json requests to parity JSON RPC API for specified blocks

  Parameters
  ----------
  parity_hosts : list
      List of tuples with each parity JSON RPC url and used block range
  blocks : list
      Block numbers
  Returns
  -------
  dict
      Urls and lists of requests attached to it
  """
  requests = {}
  for block_number in blocks:
    parity_url = _get_parity_url_by_block(parity_hosts, block_number)
    if parity_url not in requests.keys():
      requests[parity_url] = []
    requests[parity_url].append({
      "jsonrpc": "2.0",
      "id": block_number,
      "method": "trace_block",
      "params": [hex(block_number)]
    }) 
  return requests

def _get_traces_sync(parity_hosts, blocks):
  """
  Get traces for specified blocks in one array

  Parameters
  ----------
  parity_hosts : list
      List of tuples with each parity JSON RPC url and used block range. Can be found in conflg.py
  blocks : list
      Block numbers
  Returns
  -------
  list
      List of transactions inside of specified blocks
  """
  requests_dict = _make_trace_requests(parity_hosts, blocks)
  traces = []
  for parity_url, request in requests_dict.items():
    request_string = json.dumps(request)
    responses = requests.post(
      parity_url,
      data=request_string,
      headers={"content-type": "application/json"}
    ).json()
    for response in responses:
      if "result" in response.keys():
        traces += response["result"]
  return traces

class InternalTransactions:
  def __init__(self, elasticsearch_indices, elasticsearch_host="http://localhost:9200", parity_hosts=PARITY_HOSTS):
    self.indices = elasticsearch_indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.pool = Pool(processes=NUMBER_OF_PROCESSES)
    self.parity_hosts = parity_hosts

  def _split_on_chunks(self, iterable, size):
    """
    Split given iterable onto chunks

    Parameters
    ----------
    iterable : generator
        Iterable that will be splitted
    size : int
        Max size of chunk
    Returns
    -------
    generator
        Generator that returns chunk on each iteration
    """
    return utils.split_on_chunks(iterable, size)

  def _iterate_blocks(self):
    """
    Iterate through unprocessed blocks that can be found in parity nodes specified in a given config file

    Returns
    -------
    generator
        Generator that iterates through blocks without traces_extracted flag in ElasticSearch
    """
    ranges = [host_tuple[0:2] for host_tuple in self.parity_hosts]
    range_query = self.client.make_range_query("number", *ranges)
    query = {
      "query_string": {
        "query": '!(_exists_:traces_extracted) AND ' + range_query
      }
    }
    return self.client.iterate(self.indices["block"], "b", query)

  def _get_traces(self, blocks):
    """
    Get traces for specified blocks in parallel mode

    Parameters
    ----------
    blocks : list
        Block numbers
    Returns
    -------
    list
        List of transactions inside of specified blocks
    """
    chunks = self._split_on_chunks(blocks, NUMBER_OF_PROCESSES)
    arguments = list(zip(repeat(self.parity_hosts), chunks))
    traces = self.pool.starmap(_get_traces_sync, arguments)
    return [transaction for trace in traces for transaction in trace]

  def _set_trace_hashes(self, trace):
    """
    Set hash for each transaction in trace based on ethereum transaction hash
    and position in trace for this transaction

    Parameters
    ----------
    trace : list
        List of transactions
    """
    traces_size = {}
    for transaction in trace:
      transaction_hash = transaction["transactionHash"] or transaction["blockHash"]
      if transaction_hash not in traces_size.keys():
        traces_size[transaction_hash] = 0
      transaction["hash"] = "{}.{}".format(transaction_hash, traces_size[transaction_hash])
      traces_size[transaction_hash] += 1

  def _set_parent_errors(self, trace):
    """
    Set parent_error flag for all transactions in branches finished with error in trace

    Parameters
    ----------
    trace : list
        List of transactions
    """
    errors = {}
    for transaction in trace:
      if "error" in transaction.keys():
        if transaction["transactionHash"] not in errors.keys():
          errors[transaction["transactionHash"]] = trie.Trie()
        errors[transaction["transactionHash"]][transaction["traceAddress"]] = True
    for transaction in trace:
      if transaction["transactionHash"] in errors.keys():
        prefix_exists = bool(errors[transaction["transactionHash"]].shortest_prefix(transaction["traceAddress"]))
        is_node = errors[transaction["transactionHash"]].has_key(transaction["traceAddress"])
        if prefix_exists and not is_node:
          transaction["parent_error"] = True

  def _save_traces(self, blocks):
    """
    Save traces_extracted flag for processed blocks in ElasticSearch

    Parameters
    ----------
    blocks : list
        Block numbers to process
    """
    query = {
      "terms": {
        "number": blocks
      }
    }
    self.client.update_by_query(self.indices["block"], 'b', query, 'ctx._source.traces_extracted = true')

  def _preprocess_internal_transaction(self, transaction):
    """
    Preprocess specified transaction

    Flatten 'action' and 'result' field, convert value in wei to eth

    Parameters
    ----------
    transaction : dict
        Transactions to process
    """
    transaction = transaction.copy()
    for field in ["action", "result"]:
      if (field in transaction.keys()) and (transaction[field]):
        transaction.update(transaction[field])
        del transaction[field]
    if "value" in transaction.keys():
      if transaction["value"] == "0x":
        transaction["value"] = 0
      else:
        transaction["value"] = int(transaction["value"], 0) / 1e18
    return transaction

  def _save_internal_transactions(self, blocks_traces):
    """
    Save specified transactions to ElasticSearch in multiple chunks

    Save only those which are attached to an ethereum transaction

    Parameters
    ----------
    blocks_traces : list
        List of transactions to save
    """
    docs = [
      self._preprocess_internal_transaction(transaction)
      for transaction in blocks_traces
      if transaction["transactionHash"]
    ]
    if docs:
      for chunk in bulk_chunks(docs, None, BYTES_PER_CHUNK):
        self.client.bulk_index(docs=chunk, index=self.indices["internal_transaction"], doc_type="itx", id_field="hash", refresh=True)

  def _save_miner_transactions(self, blocks_traces):
    """
    Save transactions to ElasticSearch.

    Save only those which are not attached to any ethereum transaction

    Parameters
    ----------
    blocks_traces : list
        List of transactions to save
    """
    docs = [self._preprocess_internal_transaction(transaction) for transaction in blocks_traces if not transaction["transactionHash"]]
    self.client.bulk_index(docs=docs, index=self.indices["miner_transaction"], doc_type="tx", id_field="hash", refresh=True)

  def _save_genesis_block(self, genesis_file=GENESIS):
    genesis = json.load(open(genesis_file))
    self.client.bulk_index(docs=genesis, index=self.indices["internal_transaction"], doc_type="itx", id_field="hash", refresh=True)

  def _extract_traces_chunk(self, blocks):
    """
    Extract transactions from specified block numbers list

    Add trace hashes for each one, parent_error field
    Saves transactions as internal or miner (without ethereum transaction hash)
    Then saves a flag for processed blocks to ElasticSearch

    Parameters
    ----------
    blocks : list
        List of blocks numbers
    """
    if 0 in blocks:
      self._save_genesis_block()
    blocks_traces = self._get_traces(blocks)
    self._set_trace_hashes(blocks_traces)
    self._set_parent_errors(blocks_traces)
    self._save_internal_transactions(blocks_traces)
    self._save_miner_transactions(blocks_traces)
    self._save_traces(blocks)

  def extract_traces(self):
    """
    Extract traces to elasticsearch for all unprocessed blocks

    This function is an entry point for extract-traces operation
    """
    for blocks in self._iterate_blocks():
      blocks = [block["_source"]["number"] for block in blocks]
      self._extract_traces_chunk(blocks)
