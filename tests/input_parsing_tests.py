from contracts import ExternalContracts, InternalContracts, _get_contracts_abi_sync
import contracts
import os
import unittest
from test_utils import TestElasticSearch, mockify
from tqdm import *
import json
from unittest.mock import MagicMock, call, Mock
import ethereum
import functools
from time import sleep
import multiprocessing

class InputParsingTestCase():
  def setUp(self):
    self.contracts = self.contracts_class(
      {"contract": TEST_CONTRACTS_INDEX, self.index: TEST_TRANSACTIONS_INDEX},
      parity_hosts=[(None, None, "http://localhost:8545")]
    )
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_CONTRACTS_INDEX)
    self.client.recreate_fast_index(TEST_TRANSACTIONS_INDEX)

  def test_pool(self):
    assert self.contracts.pool._processes == 10

  def test_set_contracts_abi(self):
    contracts_abi = {"0x0": TEST_CONTRACT_ABI, "0x1": TEST_CONTRACT_ABI}
    self.contracts._set_contracts_abi(contracts_abi)
    self.assertSequenceEqual(self.contracts._contracts_abi, contracts_abi)

  def test_get_contract_abi(self):
    response = _get_contracts_abi_sync({1: TEST_CONTRACT_ADDRESS})
    self.assertSequenceEqual(response, {1: TEST_CONTRACT_ABI})

  def test_get_wrong_contract_abi(self):
    response = _get_contracts_abi_sync({"wrong": "0x0"})
    self.assertSequenceEqual(response, {"wrong": []})

  def test_get_uncached_contract_abi(self):
    try:
      os.remove("/home/noomkcalb/.quickBlocks/cache/abis/" + TEST_CONTRACT_ADDRESS + ".json")
    except:
      pass
    response = _get_contracts_abi_sync({"uncached": TEST_CONTRACT_ADDRESS})
    self.assertSequenceEqual(response, {"uncached": TEST_CONTRACT_ABI})

  def test_get_multiple_contracts_abi(self):
    response = _get_contracts_abi_sync({1: TEST_CONTRACT_ADDRESS, 2: TEST_CONTRACT_ADDRESS})
    self.assertSequenceEqual(response, {1: TEST_CONTRACT_ABI, 2: TEST_CONTRACT_ABI})

  def test_split_on_chunks(self):
    test_list = list(range(10))
    test_chunks = list(self.contracts._split_on_chunks(test_list, 3))
    self.assertSequenceEqual(test_chunks, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])

  def test_get_contracts_abi(self):
    addresses = ["address" + str(i) for i in range(100)]
    chunks = [[(0, "address1")], [(1, "address2")]]
    abis = [{1: "abi2"}, {0: "abi1"}]

    self.contracts._split_on_chunks = MagicMock(return_value=chunks)
    self.contracts.pool.map = MagicMock(return_value=abis)

    response = self.contracts._get_contracts_abi(addresses)

    self.contracts._split_on_chunks.assert_called_with([(index, address) for index, address in enumerate(addresses)], 10)
    self.contracts.pool.map.assert_called_with(contracts._get_contracts_abi_sync, [dict(chunk) for chunk in chunks])
    self.assertSequenceEqual(["abi1", "abi2"], response)

  def test_decode_inputs_batch(self):
    self.contracts._set_contracts_abi({"0x0": TEST_CONTRACT_ABI})
    response = self.contracts._decode_inputs_batch([
      ("0x0", TEST_CONTRACT_PARAMETERS),
      ("0x0", TEST_CONTRACT_PARAMETERS)
    ])
    self.assertSequenceEqual(response, [TEST_CONTRACT_DECODED_PARAMETERS, TEST_CONTRACT_DECODED_PARAMETERS])

  def test_decode_inputs_batch_timeout(self):
    def sleepy(*args):
      sleep(60)
      return "input"
    self.contracts._decode_input = MagicMock(side_effect=sleepy)
    with self.assertRaises(multiprocessing.context.TimeoutError):
      self.contracts._decode_inputs_batch([("0x0", "0x")])

  def add_contracts_with_and_without_abi(self):
    for i in tqdm(range(10)):
      self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS, "blockNumber": i}, id=i + 1, refresh=True)
    for i in tqdm(range(10)):
      self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS, "blockNumber": i, 'abi_extracted': True}, id=i + 11, refresh=True)
    for i in tqdm(range(5)):
      self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS, "blockNumber": i, 'abi_extracted': True, 'abi': True}, id=i + 21, refresh=True)
    for i in tqdm(range(5)):
      self.client.index(TEST_CONTRACTS_INDEX, 'contract', {
        'address': TEST_CONTRACT_ADDRESS,
        "blockNumber": i,
        'abi_extracted': True,
        'abi': True,
        self.doc_type + "_inputs_decoded": True
      }, id=i + 25, refresh=True)

  def test_iterate_contracts_without_abi(self):
    self.contracts = self.contracts_class(
      {"contract": TEST_CONTRACTS_INDEX, self.index: TEST_TRANSACTIONS_INDEX},
      parity_hosts=[(0, 8, "http://localhost:8545")]
    )
    self.add_contracts_with_and_without_abi()
    contracts = [c for c in self.contracts._iterate_contracts_without_abi()]
    contracts = [c["_id"] for contracts_list in contracts for c in contracts_list]
    self.assertCountEqual(contracts, [str(i) for i in range(1, 9)])

  def test_save_contracts_abi(self):
    for i in tqdm(range(10)):
      self.client.index(TEST_CONTRACTS_INDEX, 'contract', {"blockNumber": i, 'address': TEST_CONTRACT_ADDRESS}, id=i + 1, refresh=True)
    self.contracts.save_contracts_abi()
    contracts = self.client.search(index=TEST_CONTRACTS_INDEX, doc_type='contract', query="abi:*", size=100)['hits']['hits']
    abis = [contract["_source"]["abi"] for contract in contracts]
    self.assertCountEqual(abis, [TEST_CONTRACT_ABI] * 10)

  def test_save_contracts_abi_status(self):
    for i in tqdm(range(10)):
      self.client.index(TEST_CONTRACTS_INDEX, 'contract', {"blockNumber": i, 'address': TEST_CONTRACT_ADDRESS}, id=i + 1, refresh=True)
    self.contracts.save_contracts_abi()
    contracts_count = self.client.count(index=TEST_CONTRACTS_INDEX, doc_type='contract', query="abi_extracted:true")['count']
    assert contracts_count == 10

  def test_iterate_contracts_with_abi(self):
    self.contracts = self.contracts_class(
      {"contract": TEST_CONTRACTS_INDEX, self.index: TEST_TRANSACTIONS_INDEX},
      parity_hosts=[(0, 4, "http://localhost:8545")]
    )
    self.add_contracts_with_and_without_abi()
    contracts = [c for c in self.contracts._iterate_contracts_with_abi()]
    contracts = [c["_id"] for contracts_list in contracts for c in contracts_list]
    self.assertCountEqual(contracts, [str(i) for i in range(21, 25)])

  def test_decode_inputs_for_contracts(self):
    self.contracts._set_contracts_abi({TEST_CONTRACT_ADDRESS: TEST_CONTRACT_ABI})
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS, 'abi': TEST_CONTRACT_ABI}, id=1, refresh=True)
    for i in tqdm(range(10)):
      self.client.index(TEST_TRANSACTIONS_INDEX, self.doc_type, self.doc, id=i + 1, refresh=True)
    contracts = self.client.search(index=TEST_CONTRACTS_INDEX, doc_type='contract', query="abi:*")['hits']['hits']
    self.contracts._decode_inputs_for_contracts(contracts)
    transactions = self.client.search(index=TEST_TRANSACTIONS_INDEX, doc_type=self.doc_type, query="*")['hits']['hits']
    decoded_inputs = [t["_source"]["decoded_input"] for t in transactions]
    self.assertCountEqual(decoded_inputs, [TEST_CONTRACT_DECODED_PARAMETERS] * 10)

  def test_decode_inputs_for_contracts_exception(self):
    def exception_on_seven(inputs):
      if inputs[0][1] == 7:
        raise multiprocessing.context.TimeoutError()
      return ["input"] * len(inputs)
    test_transactions = [[{
      "_id": i*10 + j,
      "_source": {
        "to": i,
        "input": j
      }
    } for i in range(10)] for j in range(10)]
    self.contracts._iterate_transactions_by_targets = MagicMock(return_value=test_transactions)
    self.contracts.client.bulk = MagicMock()
    self.contracts._decode_inputs_batch = MagicMock(side_effect=exception_on_seven)

    self.contracts._decode_inputs_for_contracts([])

    assert self.contracts.client.bulk.call_count == 9

  def test_save_inputs_decoded(self):
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS + str(1)}, id=1, refresh=True)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS + str(2)}, id=2, refresh=True)
    self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS + str(3)}, id=3, refresh=True)
    self.contracts._save_inputs_decoded([TEST_CONTRACT_ADDRESS + str(1), TEST_CONTRACT_ADDRESS + str(3)])
    contracts = self.client.search(index=TEST_CONTRACTS_INDEX, doc_type='contract', query=self.doc_type + "_inputs_decoded:true")['hits']['hits']
    contracts = [contract["_source"]["address"] for contract in contracts]
    self.assertCountEqual(contracts, [TEST_CONTRACT_ADDRESS + str(1), TEST_CONTRACT_ADDRESS + str(3)])

  def test_decode_inputs_save_inputs_decoded(self):
    test_contracts = ["contract1", "contract2", "contract3"]
    test_contracts_from_elasticsearch = [{"_source": {"abi": contract, "address": contract}} for contract in test_contracts]
    mockify(self.contracts, {
      "_iterate_contracts_with_abi": MagicMock(return_value=[test_contracts_from_elasticsearch])
    }, ["decode_inputs"])
    process = Mock(
      decode=self.contracts._decode_inputs_for_contracts,
      save=self.contracts._save_inputs_decoded
    )

    self.contracts.decode_inputs()

    process.assert_has_calls([
      call.decode(test_contracts_from_elasticsearch),
      call.save(test_contracts)
    ])

  def test_decode_inputs_for_big_portion_of_contracts(self):
    for i in tqdm(range(10)):
      self.client.index(TEST_CONTRACTS_INDEX, 'contract', {'address': TEST_CONTRACT_ADDRESS, 'blockNumber': i}, id=i + 1, refresh=True)
    for i in tqdm(range(10)):
      self.client.index(TEST_TRANSACTIONS_INDEX, self.doc_type, self.doc, id=i + 1, refresh=True)
    self.contracts.save_contracts_abi()
    self.contracts.decode_inputs()
    transactions = self.client.search(index=TEST_TRANSACTIONS_INDEX, doc_type=self.doc_type, query="*")['hits']['hits']
    assert len([transaction["_source"]["decoded_input"] for transaction in transactions]) == 10

TEST_CONTRACT_ABI = json.loads('[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"bytes32"}],"payable":false,"type":"function"},{"constant":false,"inputs":[],"name":"stop","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"owner_","type":"address"}],"name":"setOwner","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint128"}],"name":"push","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"name_","type":"bytes32"}],"name":"setName","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint128"}],"name":"mint","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"src","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"stopped","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"authority_","type":"address"}],"name":"setAuthority","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"wad","type":"uint128"}],"name":"pull","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint128"}],"name":"burn","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"bytes32"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"type":"function"},{"constant":false,"inputs":[],"name":"start","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"authority","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"src","type":"address"},{"name":"guy","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"inputs":[{"name":"symbol_","type":"bytes32"}],"payable":false,"type":"constructor"},{"anonymous":true,"inputs":[{"indexed":true,"name":"sig","type":"bytes4"},{"indexed":true,"name":"guy","type":"address"},{"indexed":true,"name":"foo","type":"bytes32"},{"indexed":true,"name":"bar","type":"bytes32"},{"indexed":false,"name":"wad","type":"uint256"},{"indexed":false,"name":"fax","type":"bytes"}],"name":"LogNote","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"authority","type":"address"}],"name":"LogSetAuthority","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"}],"name":"LogSetOwner","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"}]')
TEST_CONTRACT_ADDRESS = '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0'
TEST_CONTRACT_PARAMETERS = '0xa9059cbb000000000000000000000000d11b80088ce2623a9c017b93008405511cd951d200000000000000000000000000000000000000000000000d343b16da9c1a4000'
TEST_CONTRACT_DECODED_PARAMETERS = {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xd11b80088ce2623a9c017b93008405511cd951d2'}, {'type': 'uint256', 'value': '243571300000000000000'}]}
TEST_TRANSACTIONS_INDEX = 'test-ethereum-transactions'
TEST_CONTRACTS_INDEX = 'test-ethereum-contracts'

class ExternalTransactionsInputParsingTestCase(InputParsingTestCase, unittest.TestCase):
  doc_type = "tx"
  index = "transaction"
  contracts_class = ExternalContracts
  doc = {'to': TEST_CONTRACT_ADDRESS, 'input': TEST_CONTRACT_PARAMETERS}

  def test_iterate_transactions_by_targets(self):
    for i in tqdm(range(20)):
      self.client.index(TEST_TRANSACTIONS_INDEX, self.doc_type, {'to': TEST_CONTRACT_ADDRESS}, id=i + 1, refresh=True)
    for i in tqdm(range(20)):
      self.client.index(TEST_TRANSACTIONS_INDEX, self.doc_type, {'to': "0x"}, id=i + 21, refresh=True)
    targets = [TEST_CONTRACT_ADDRESS]
    transactions = [c for c in self.contracts._iterate_transactions_by_targets(targets)]
    transactions = [t["_id"] for transactions_list in transactions for t in transactions_list]
    self.assertCountEqual(transactions, [str(i) for i in range(1, 21)])

class InternalTransactionsInputParsingTestCase(InputParsingTestCase, unittest.TestCase):
  doc_type = "itx"
  index = "internal_transaction"
  contracts_class = InternalContracts
  doc = {'to': TEST_CONTRACT_ADDRESS, 'input': TEST_CONTRACT_PARAMETERS, "callType": "call"}

  def test_iterate_transactions_by_targets(self):
    self.client.index(TEST_TRANSACTIONS_INDEX, self.doc_type, {'to': TEST_CONTRACT_ADDRESS, "callType": "delegatecall"}, id=1, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, self.doc_type, {'to': TEST_CONTRACT_ADDRESS, 'callType': 'call'}, id=2, refresh=True)
    self.client.index(TEST_TRANSACTIONS_INDEX, self.doc_type, {'to': "0x"}, id=3, refresh=True)
    targets = [TEST_CONTRACT_ADDRESS]
    transactions = [c for c in self.contracts._iterate_transactions_by_targets(targets)]
    transactions = [t["_id"] for transactions_list in transactions for t in transactions_list]
    self.assertCountEqual(transactions, ['2'])