from operations.inputs import ClickhouseEventsInputs, ClickhouseTransactionsInputs
from operations import inputs
import os
import unittest
from tests.test_utils import TestElasticSearch, mockify, TestClickhouse
from tqdm import *
from unittest.mock import MagicMock, call, Mock, patch, ANY
import multiprocessing
import json
from operations.indices import ClickhouseIndices

TEST_CONTRACT_ABI = json.loads('[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"bytes32"}],"payable":false,"type":"function"},{"constant":false,"inputs":[],"name":"stop","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"owner_","type":"address"}],"name":"setOwner","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint128"}],"name":"push","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"name_","type":"bytes32"}],"name":"setName","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint128"}],"name":"mint","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"src","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"stopped","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"authority_","type":"address"}],"name":"setAuthority","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"wad","type":"uint128"}],"name":"pull","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint128"}],"name":"burn","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"bytes32"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[],"name":"start","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"authority","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"src","type":"address"},{"name":"guy","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"inputs":[{"name":"symbol_","type":"bytes32"}],"payable":false,"type":"constructor"},{"anonymous":true,"inputs":[{"indexed":true,"name":"sig","type":"bytes4"},{"indexed":true,"name":"guy","type":"address"},{"indexed":true,"name":"foo","type":"bytes32"},{"indexed":true,"name":"bar","type":"bytes32"},{"indexed":false,"name":"wad","type":"uint256"},{"indexed":false,"name":"fax","type":"bytes"}],"name":"LogNote","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"authority","type":"address"}],"name":"LogSetAuthority","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"}],"name":"LogSetOwner","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"}]')
TEST_CONTRACT_ADDRESS = '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0'
TEST_CONTRACT_PARAMETERS = '0xa9059cbb000000000000000000000000d11b80088ce2623a9c017b93008405511cd951d200000000000000000000000000000000000000000000000d343b16da9c1a4000'
TEST_CONTRACT_DECODED_PARAMETERS = {
  'name': 'transfer',
  'params.type': ['address', 'uint256'],
  'params.value': ['0xd11b80088ce2623a9c017b93008405511cd951d2', '243571300000000000000'],
}
TEST_TRANSACTIONS_INDEX = 'test_ethereum_transactions'
TEST_CONTRACTS_INDEX = 'test_ethereum_contracts'
TEST_CONTRACTS_ABI_INDEX = 'test_ethereum_contracts_abi'
TEST_CONTRACT_BLOCK_INDEX = 'test_ethereum_contract_block'
TEST_TRANSACTIONS_INPUT_INDEX = 'test_transactions_input'
TEST_BLOCKS_INDEX = 'test_ethereum_blocks'
TEST_BLOCKS_FLAG_INDEX = 'test_ethereum_blocks_flag'

class ClickhouseInputParsingTestCase():
  def setUp(self):
    self.client = TestClickhouse()
    self.indices = {
      "contract": TEST_CONTRACTS_INDEX,
      self.index: TEST_TRANSACTIONS_INDEX,
      "contract_abi": TEST_CONTRACTS_ABI_INDEX,
      "contract_block": TEST_CONTRACT_BLOCK_INDEX,
      self.input_index: TEST_TRANSACTIONS_INPUT_INDEX,
      "block": TEST_BLOCKS_INDEX,
      "block_flag": TEST_BLOCKS_FLAG_INDEX
    }
    self.contracts = self.contracts_class(
      self.indices,
      parity_hosts=[(None, None, "http://localhost:8545")]
    )
    self.contracts.doc_type = self.doc_type
    self.contracts.index = self.index
    self.contracts.input_index = self.input_index
    self.client.prepare_indices(self.indices)

  def test_set_contracts_abi(self):
    """Test setting contracts ABI"""
    contracts_abi = {"0x0": json.dumps(TEST_CONTRACT_ABI), "0x1": json.dumps(TEST_CONTRACT_ABI)}
    self.contracts._set_contracts_abi(contracts_abi)
    self.assertSequenceEqual(self.contracts._contracts_abi, {
      "0x0": TEST_CONTRACT_ABI,
      "0x1": TEST_CONTRACT_ABI
    })

  def test_decode_inputs_batch_sync(self):
    """Test decode inputs batch"""
    response = inputs._decode_inputs_batch_sync({
      "0x1": (TEST_CONTRACT_ABI, TEST_CONTRACT_PARAMETERS),
      "0x2": (TEST_CONTRACT_ABI, TEST_CONTRACT_PARAMETERS)
    })
    self.assertSequenceEqual(response, {
      "0x1": TEST_CONTRACT_DECODED_PARAMETERS,
      "0x2": TEST_CONTRACT_DECODED_PARAMETERS
    })

  def test_decode_inputs_batch(self):
    """Test decoding inputs batch in parallel mode"""
    test_inputs = {"0x" + str(i): "input" + str(i) for i in range(100)}
    chunks = [[("0x1", "input1")], [("0x1", "input2")]]
    decoded_inputs = [{"0x1": "decoded_input2"}, {"0x0": "decoded_input1"}]

    self.contracts._split_on_chunks = MagicMock(return_value=chunks)
    self.contracts.pool.map = MagicMock(return_value=decoded_inputs)

    response = self.contracts._decode_inputs_batch(test_inputs)

    self.contracts._split_on_chunks.assert_called_with([(hash, input) for hash, input in test_inputs.items()], 10)
    self.contracts.pool.map.assert_called_with(inputs._decode_inputs_batch_sync, [dict(chunk) for chunk in chunks])
    self.assertSequenceEqual({"0x0": "decoded_input1", "0x1": "decoded_input2"}, response)

  def add_contracts_with_and_without_abi(self):
    """Add 10 contracts with no ABI at all, 10 contracts with abi_extracted flag and 5 contracts with ABI"""
    contracts = [{'address': TEST_CONTRACT_ADDRESS, "blockNumber": i % 5, "id": i + 1} for i in range(25)]
    contracts_abi = [{'abi_extracted': True, 'id': i} for i in range(11, 21)]
    contracts_abi += [{'abi_extracted': True, 'abi': json.dumps({"test": 1}), 'id': i} for i in range(21, 26)]
    self.client.bulk_index(TEST_CONTRACTS_INDEX, contracts)
    self.client.bulk_index(TEST_CONTRACTS_ABI_INDEX, contracts_abi)

  def test_iterate_contracts_with_abi(self):
    """Test iterations through contracts with ABI"""
    test_max_block = 100
    self.contracts = self.contracts_class(
      self.indices,
      parity_hosts=[(0, 4, "http://localhost:8545")]
    )
    self.add_contracts_with_and_without_abi()
    contracts = [c for c in self.contracts._iterate_contracts_with_abi(test_max_block)]
    contracts = [c["_id"] for contracts_list in contracts for c in contracts_list]
    self.assertCountEqual(contracts, [str(i) for i in range(21, 25)])

  def test_iterate_contracts_with_abi_call_iterate_contracts(self):
    """Test iterations through contracts with unprocessed transactions before some block"""
    test_max_block = 2
    test_iterator = "iterator"
    self.contracts._iterate_contracts = MagicMock(return_value=test_iterator)

    contracts = self.contracts._iterate_contracts_with_abi(test_max_block)

    self.contracts._iterate_contracts.assert_any_call(test_max_block, ANY, fields=ANY)
    assert contracts == test_iterator

  def test_iterate_transactions_by_targets_select_unprocessed_transactions(self):
    """Test iterations through transactions before specified block"""
    test_iterator = []
    test_max_block = 0
    test_contracts = ["contract"]
    self.contracts._iterate_transactions = MagicMock(return_value=test_iterator)

    transactions = self.contracts._iterate_transactions_by_targets(test_contracts, test_max_block)
    transactions = [t for t in transactions]

    self.contracts._iterate_transactions.assert_any_call(test_contracts, test_max_block, ANY, fields=ANY)
    assert transactions == test_iterator

  def test_decode_inputs_for_contracts(self):
    """Test decoding inputs for contracts chunk from elasticsearch"""
    test_max_block = 1000
    self.contracts._set_contracts_abi({TEST_CONTRACT_ADDRESS: json.dumps(TEST_CONTRACT_ABI)})
    self.client.index(TEST_CONTRACTS_INDEX, {'address': TEST_CONTRACT_ADDRESS}, id=1)
    self.client.index(TEST_CONTRACTS_ABI_INDEX, {"abi": json.dumps(TEST_CONTRACT_ABI)}, id=1)
    for i in tqdm(range(10)):
      self.client.index(TEST_TRANSACTIONS_INDEX, self.doc, id=i + 1)
    contracts = self.client.search(
      index=TEST_CONTRACTS_INDEX,
      query="ANY INNER JOIN {} USING id".format(TEST_CONTRACTS_ABI_INDEX),
      fields=["address"]
    )
    self.contracts._decode_inputs_for_contracts(contracts, test_max_block)
    transactions = self.client.search(
      index=TEST_TRANSACTIONS_INPUT_INDEX,
      fields=["params.type", "params.value", "name"]
    )
    # TODO remove a JSON convertation hack to replace tuples with arrays
    decoded_inputs = [json.loads(json.dumps(t["_source"])) for t in transactions]
    self.assertCountEqual(decoded_inputs, [TEST_CONTRACT_DECODED_PARAMETERS] * 10)

  def test_decode_inputs_for_contracts_iterate_arguments(self):
    """Test arguments for iterate method (should pass contracts and max block)"""
    test_contracts = [
      {"_source": {"address": "0x" + str(i), self.doc_type + "_inputs_decoded_block": i}}
      for i in range(10)
    ]
    test_contracts.append({"_source": {"address": "0xa"}})
    test_block = 100
    mock_iterate = MagicMock(return_value=[])
    mockify(self.contracts, {
      "_iterate_transactions_by_targets": mock_iterate
    }, "_decode_inputs_for_contracts")

    self.contracts._decode_inputs_for_contracts(test_contracts, test_block)

    mock_iterate.assert_called_with(test_contracts, test_block)

  def test_decode_inputs_for_contracts_exception(self):
    """Test exception for some input in chunk"""
    def exception_on_seven(inputs):
      if "input7" in str(inputs):
        raise multiprocessing.context.TimeoutError()
      return {key: "input" for key in inputs}

    test_transactions = [[{
      "_id": i*10 + j,
      "_source": {**self.doc, **{
        "input": "input" + str(j)
      }}
    } for i in range(10)] for j in range(10)]
    self.contracts._set_contracts_abi({TEST_CONTRACT_ADDRESS: json.dumps({"abi": i}) for i in range(10)})
    self.contracts._iterate_transactions_by_targets = MagicMock(return_value=test_transactions)
    self.contracts._add_id_to_inputs = MagicMock()
    self.contracts.client.bulk_index = MagicMock()
    self.contracts._decode_inputs_batch = MagicMock(side_effect=exception_on_seven)

    self.contracts._decode_inputs_for_contracts([], ANY)

    assert self.contracts.client.bulk_index.call_count == 9

  def test_decode_inputs_save_inputs_decoded(self):
    """Test saving decoded inputs in process"""
    test_contracts = ["contract1", "contract2", "contract3"]
    test_contracts_from_elasticsearch = [{"_source": {"abi": contract, "address": contract}} for contract in test_contracts]
    mockify(self.contracts, {
      "_iterate_contracts_with_abi": MagicMock(return_value=[test_contracts_from_elasticsearch]),
      "_get_max_block": MagicMock()
    }, ["decode_inputs"])
    process = Mock(
      decode=self.contracts._decode_inputs_for_contracts,
      save=self.contracts._save_max_block
    )

    self.contracts.decode_inputs()

    process.assert_has_calls([
      call.decode(test_contracts_from_elasticsearch, ANY),
      call.save(test_contracts, ANY)
    ])

  def test_decode_inputs_save_max_block_for_query(self):
    """Test saving max block parameter during all operation"""
    test_max_block = 1000
    test_contracts_from_elasticsearch = [{"_source": {"abi": "contract", "address": "contract" + str(i)}} for i in range(3)]
    mockify(self.contracts, {
      "_iterate_contracts_with_abi": MagicMock(return_value=[test_contracts_from_elasticsearch]),
      "_get_max_block": MagicMock(side_effect=[test_max_block])
    }, ["decode_inputs"])
    process = Mock(
      iterate=self.contracts._iterate_contracts_with_abi,
      decode=self.contracts._decode_inputs_for_contracts,
      save=self.contracts._save_max_block
    )
    self.contracts.decode_inputs()

    self.contracts._get_max_block.assert_called_with(ANY)
    process.assert_has_calls([
      call.iterate(test_max_block),
      call.decode(ANY, test_max_block),
      call.save(ANY, test_max_block)
    ])

  def test_decode_inputs_use_block_query_for_max_block(self):
    test_blocks = [{
      "id": 1,
      "number": 1
    }, {
      "id": 2,
      "number": 2
    }]
    test_block_flags = [{
      "id": 1,
      "name": self.block_flag_name,
      "value": 1
    }]
    test_max_block = 1
    self.client.bulk_index(index=TEST_BLOCKS_INDEX, docs=test_blocks)
    self.client.bulk_index(index=TEST_BLOCKS_FLAG_INDEX, docs=test_block_flags)
    self.contracts._iterate_contracts_with_abi = MagicMock(return_value=[])

    self.contracts.decode_inputs()

    self.contracts._iterate_contracts_with_abi.assert_called_with(test_max_block)

  def test_decode_inputs_for_big_portion_of_contracts(self):
    """Test decoding inputs for big portion of contracts in ElasticSearch"""
    for i in tqdm(range(10)):
      self.client.index(TEST_CONTRACTS_INDEX, {'address': TEST_CONTRACT_ADDRESS, 'blockNumber': i}, id=i + 1)
    for i in tqdm(range(10)):
      self.client.index(TEST_CONTRACTS_ABI_INDEX, {'abi': json.dumps(TEST_CONTRACT_ABI)}, id=i + 1)
    for i in tqdm(range(10)):
      self.client.index(TEST_TRANSACTIONS_INDEX, self.doc, id=i + 1)
    self.contracts._get_max_block = MagicMock(return_value=1000)
    self.contracts.decode_inputs()

    transactions = self.client.search(index=TEST_TRANSACTIONS_INPUT_INDEX, fields=["name"])
    assert len([transaction["_source"] for transaction in transactions]) == 10

class ClickhouseTransactionsInputParsingTestCase(ClickhouseInputParsingTestCase, unittest.TestCase):
  doc = {
    'to': TEST_CONTRACT_ADDRESS,
    'input': TEST_CONTRACT_PARAMETERS,
    "callType": "call",
    'blockNumber': 10
  }
  contracts_class = ClickhouseTransactionsInputs
  doc_type = "itx"
  index = "internal_transaction"
  input_index = "transaction_input"
  block_flag_name = "traces_extracted"

  def test_iterate_transactions_by_targets_ignore_transactions_with_error(self):
    """Test iterations through CALL EVM transactions without errors"""
    self.contracts._create_transactions_request = MagicMock(return_value="id IS NOT NULL")
    test_transactions = [{
      "id": 1,
      "callType": "call"
    }, {
      "id": 2,
      "callType": "delegatecall"
    }, {
      "id": 3,
      "callType": "call",
      "error": "Out of gas"
    }]

    self.client.bulk_index(index=TEST_TRANSACTIONS_INDEX, docs=test_transactions)
    targets = [{"_source": {"address": TEST_CONTRACT_ADDRESS}}]
    transactions = self.contracts._iterate_transactions_by_targets(targets, 0)
    transactions = [t["_id"] for transactions_list in transactions for t in transactions_list]
    self.assertCountEqual(transactions, ['1'])

class ClickhouseEventsInputParsingTestCase(ClickhouseInputParsingTestCase, unittest.TestCase):
  doc = {
    'address': TEST_CONTRACT_ADDRESS,
    'topics': [
      TEST_CONTRACT_PARAMETERS[0:10],
      "0x" + TEST_CONTRACT_PARAMETERS[10:32+10],
      "0x" + TEST_CONTRACT_PARAMETERS[32 + 10:64 + 10],
      "0x" + TEST_CONTRACT_PARAMETERS[64 + 10:96 + 10],
    ],
    'data': "0x" + TEST_CONTRACT_PARAMETERS[96 + 10:96 + 32 + 10],
    'blockNumber': 10
  }
  contracts_class = ClickhouseEventsInputs
  doc_type = "event"
  index = "event"
  input_index = "event_input"
  block_flag_name = "events_extracted"

  def test_iterate_transactions_restore_input_field(self):
    test_transactions = [{"_source": self.doc}]
    self.contracts._iterate_transactions = MagicMock(return_value=[test_transactions])
    transactions = self.contracts._iterate_transactions_by_targets([], 0)
    transaction = next(transactions)[0]
    assert transaction["_source"]["input"] == TEST_CONTRACT_PARAMETERS

