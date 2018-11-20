import unittest
from operations.multitransfers_detection import ClickhouseMultitransfersDetection
from tests.test_utils import TestClickhouse
from sklearn.tree import DecisionTreeClassifier
from sklearn.externals import joblib
from unittest.mock import MagicMock, Mock, call, patch
import pandas as pd
import numpy as np
from tests.test_utils import mockify

from clickhouse_driver import Client
import config

class MultitransfersDetectionTestCase(unittest.TestCase):
  test_transfer_event_hex = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
  maxDiff = None
  def setUp(self):
    self.indices = {
      "multitransfer": TEST_MULTITRANSFERS_INDEX,
      "contract": TEST_CONTRACTS_INDEX,
      "internal_transaction": TEST_TRANSACTIONS_INDEX,
      "event": TEST_EVENTS_INDEX,
      "contract_block": "mock"
    }
    self.multitransfers_detection = ClickhouseMultitransfersDetection(self.indices)
    self.client = TestClickhouse()
    self.client.prepare_indices(self.indices)

  def test_iterate_tokens(self):
    test_contracts = [{
      "id": 1,
      "address": "0x1",
      "standards": ["erc20"]
    }, {
      "id": 2,
      "address": "0x2",
      "standards": [None]
    }]
    self.client.bulk_index(index=TEST_CONTRACTS_INDEX, docs=test_contracts)

    iterator = self.multitransfers_detection._iterate_tokens()
    tokens = [token["_source"]["address"] for token_list in iterator for token in token_list]
    self.assertSequenceEqual(tokens, ["0x1"])

  def test_iterate_tokens_from_whitelist(self):
    config.PROCESSED_CONTRACTS.append("0x1")
    test_contracts = [{
      "id": 1,
      "address": "0x1",
      "standards": ["erc20"]
    }, {
      "id": 2,
      "address": "0x2",
      "standards": ["erc20"]
    }]
    self.client.bulk_index(index=TEST_CONTRACTS_INDEX, docs=test_contracts)

    iterator = self.multitransfers_detection._iterate_tokens()
    tokens = [token["_source"]["address"] for token_list in iterator for token in token_list]
    self.assertSequenceEqual(tokens, ["0x1"])
    config.PROCESSED_CONTRACTS.clear()

  def test_find_top_addresses(self):
    test_transactions = [{
      "id": 0,
      "from": "0x1",
      "to": "0x2"
    }, {
      "id": 1,
      "from": "0x1",
      "to": "0x2"
    }, {
      "id": 2,
      "from": "0x3",
      "to": "0x2"
    }, {
      "id": 3,
      "from": "0x1",
      "to": "0x2"
    }, {
      "id": 4,
      "from": "0x3",
      "to": "0x3"
    }, {
      "id": 5,
      "from": "0x3",
      "to": "0x3"
    }, {
      "id": 6,
      "from": "0x3",
      "to": "0x2"
    }, {
      "id": 7,
      "from": "0x4",
      "to": "0x2"
    }]

    self.client.bulk_index(index=TEST_TRANSACTIONS_INDEX, docs=test_transactions)

    self.multitransfers_detection._iterate_tokens = MagicMock(return_value=[[
      {"_source": {"address": "0x2"}}
    ]])

    top_addresses = next(self.multitransfers_detection._iterate_top_addresses("0x2", limit=2))

    self.assertSequenceEqual(["0x1", "0x3"], [address["_source"]["address"] for address in top_addresses])

  def _create_event_address(self, address):
    hex_length = len("0x000000000000000000000000d6cd31f283d24cfb442cba1bcf42290c07c15792")
    return "{0:#0{1}x}".format(int(address, 0), hex_length)

  def _create_transaction_address(self, address):
    hex_length = 42
    return "{0:#0{1}x}".format(int(address, 0), hex_length)

  def test_get_events_per_transaction(self):
    test_transfer_event_hex = self.test_transfer_event_hex
    test_events = [{
      "id": 1,
      "address": "0x01",
      "topics": [test_transfer_event_hex, self._create_event_address("0x1")],
      "transactionHash": "0xf1"
    }, {
      "id": 2,
      "address": "0x01",
      "topics": [test_transfer_event_hex, self._create_event_address("0x2")],
      "transactionHash": "0xf2"
    }, {
      "id": 3,
      "address": "0x01",
      "topics": [test_transfer_event_hex, self._create_event_address("0x2")],
      "transactionHash": "0xf2"
    }, {
      "id": 4,
      "address": "0x02",
      "topics": [test_transfer_event_hex, self._create_event_address("0x3")],
      "transactionHash": "0xf3"
    }, {
      "id": 5,
      "address": "0x01",
      "topics": ["0x0", self._create_event_address("0x4")],
      "transactionHash": "0xf4"
    }, {
      "id": 6,
      "address": "0x01",
      "topics": [test_transfer_event_hex, self._create_event_address("0x1")],
      "transactionHash": "0xf5"
    }, {
      "id": 7,
      "address": "0x01",
      "topics": [test_transfer_event_hex, self._create_event_address("0x2")],
      "transactionHash": "0xf5"
    }, {
      "id": 8,
      "address": "0x01",
      "topics": [test_transfer_event_hex, self._create_event_address("0x5")],
      "transactionHash": "0xf6"
    }]
    self.client.bulk_index(index=TEST_EVENTS_INDEX, docs=test_events)
    test_token = "0x01"
    test_addresses = [self._create_transaction_address("0x1"), self._create_transaction_address("0x2")]

    result = self.multitransfers_detection._get_events_per_transaction(test_token, test_addresses).to_dict()

    self.assertSequenceEqual(result, {
      self._create_transaction_address("0x1"): 1,
      self._create_transaction_address("0x2"): 2
    })

  def test_get_ethereum_senders(self):
    test_transactions = [{
      "id": 1,
      "to":  self._create_transaction_address("0x1"),
      "from": self._create_transaction_address("0x2"),
      "value": 1
    }, {
      "id": 2,
      "to":  self._create_transaction_address("0x1"),
      "from": self._create_transaction_address("0x3"),
      "value": 2
    }, {
      "id": 3,
      "to":  self._create_transaction_address("0x2"),
      "from": self._create_transaction_address("0x2"),
      "value": 3,
    }, {
      "id": 4,
      "to":  self._create_transaction_address("0x1"),
      "from": self._create_transaction_address("0x4"),
      "value": 0
    }, {
      "id": 5,
      "to": self._create_transaction_address("0x1"),
      "from": self._create_transaction_address("0x5"),
      "value": 1
    }]
    test_events = [{
      "id": 1,
      "address": "0x01",
      "topics": [self.test_transfer_event_hex, "0x0", self._create_event_address("0x2")]
    }, {
      "id": 2,
      "address": "0x01",
      "topics": [self.test_transfer_event_hex, "0x0", self._create_event_address("0x4")]
    }, {
      "id": 3,
      "address": "0x01",
      "topics": ["0x0", "0x0", self._create_event_address("0x3")]
    }, {
      "id": 4,
      "address": "0x02",
      "topics": [self.test_transfer_event_hex, "0x0", self._create_event_address("0x3")]
    }]
    self.client.bulk_index(index=TEST_TRANSACTIONS_INDEX, docs=test_transactions)
    self.client.bulk_index(index=TEST_EVENTS_INDEX, docs=test_events)
    test_token = "0x01"
    test_distributors = [ self._create_transaction_address("0x1"),  self._create_transaction_address("0x2")]

    result = self.multitransfers_detection._get_ethereum_senders(test_token, test_distributors).to_dict()

    print(result)
    self.assertSequenceEqual(result, {
      self._create_transaction_address("0x1"): 1 / (1 + 2),
      self._create_transaction_address("0x2"): 1 / 1
    })

  def test_get_token_receivers(self):
    test_events = [{
      "id": 1,
      "address": "0x01",
      "topics": [self.test_transfer_event_hex, self._create_event_address("0x1"), self._create_event_address("0x2")]
    }, {
      "id": 2,
      "address": "0x01",
      "topics": [self.test_transfer_event_hex, self._create_event_address("0x1"), self._create_event_address("0x3")]
    }, {
      "id": 3,
      "address": "0x01",
      "topics": ["0x0", self._create_event_address("0x1"), self._create_event_address("0x4")]
    }, {
      "id": 4,
      "address": "0x02",
      "topics": [self.test_transfer_event_hex, self._create_event_address("0x1"), self._create_event_address("0x5")]
    }, {
      "id": 5,
      "address": "0x01",
      "topics": [self.test_transfer_event_hex, self._create_event_address("0x2"), self._create_event_address("0x6")]
    }]

    self.client.bulk_index(index=TEST_EVENTS_INDEX, docs=test_events)
    test_token = "0x01"
    test_distributors = [self._create_transaction_address("0x1"), self._create_transaction_address("0x2")]

    result = self.multitransfers_detection._get_token_receivers(test_token, test_distributors).to_dict()

    self.assertSequenceEqual(result, {
      self._create_transaction_address("0x1"): 2 / 3,
      self._create_transaction_address("0x2"): 1 / 3
    })

  def test_get_initiated_holders(self):
    test_events = [{
      "id": 1,
      "address": "0x01",
      "blockNumber": 1,
      "topics": [self.test_transfer_event_hex, self._create_event_address("0x1"), self._create_event_address("0x2")]
    }, {
      "id": 2,
      "address": "0x01",
      "blockNumber": 2,
      "topics": [self.test_transfer_event_hex, self._create_event_address("0x2"), self._create_event_address("0x3")]
    }, {
      "id": 3,
      "blockNumber": 3,
      "address": "0x01",
      "topics": [self.test_transfer_event_hex, self._create_event_address("0x1"), self._create_event_address("0x3")]
    }, {
      "id": 4,
      "blockNumber": 4,
      "address": "0x02",
      "topics": [self.test_transfer_event_hex, self._create_event_address("0x1"), self._create_event_address("0x4")]
    }, {
      "id": 5,
      "blockNumber": 5,
      "address": "0x01",
      "topics": ["0x0", self._create_event_address("0x1"), self._create_event_address("0x5")]
    }]
    self.client.bulk_index(index=TEST_EVENTS_INDEX, docs=test_events)
    test_token = "0x01"
    test_addresses = [self._create_transaction_address("0x1"), self._create_transaction_address("0x2")]

    result = self.multitransfers_detection._get_initiated_holders(test_token, test_addresses).to_dict()

    self.assertSequenceEqual(result, {
      self._create_transaction_address("0x1"): 1 / 2,
      self._create_transaction_address("0x2"): 1 / 2
    })

  def test_get_features(self):
    test_tokens = ["0x01", "0x02"]
    test_client_tokens = [{"_source": {"address": token}} for token in test_tokens]
    test_addresses = [
      ["0x1", "0x2"],
      ["0x2", "0x3"]
    ]
    test_client_addresses = [
      [[{"_source": {"address": address}} for address in address_list]]
      for address_list in test_addresses
    ]
    self.multitransfers_detection._iterate_tokens = MagicMock(return_value=test_client_tokens)
    self.multitransfers_detection._iterate_top_addresses = MagicMock(side_effect=test_client_addresses)
    test_feature = pd.DataFrame([["0x2", 2], ["0x1", 1]], columns=["address", "feature"]).set_index("address")["feature"]
    feature_mock = MagicMock(return_value=test_feature)
    self.multitransfers_detection._get_ethereum_senders = feature_mock
    self.multitransfers_detection._get_events_per_transaction = feature_mock
    self.multitransfers_detection._get_token_receivers = feature_mock
    self.multitransfers_detection._get_initiated_holders = feature_mock

    result = self.multitransfers_detection._get_features(test_client_tokens)
    calls = []
    for token, addresses in zip(test_tokens, test_addresses):
      calls += [call(token, addresses)]
    feature_mock.assert_has_calls(calls)
    
    result_records = result.reset_index().to_dict('records')
    result_ids = result.index
    self.assertCountEqual(result_records, [{
      'token': "0x01",
      "address": "0x1",
      "ethereum_senders": 1,
      "events_per_transaction": 1,
      "token_receivers": 1,
      "initiated_holders": 1
    }, {
      'token': "0x01",
      "address": "0x2",
      "ethereum_senders": 2,
      "events_per_transaction": 2,
      "token_receivers": 2,
      "initiated_holders": 2
    }, {
      'token': "0x02",
      "address": "0x2",
      "ethereum_senders": 2,
      "events_per_transaction": 2,
      "token_receivers": 2,
      "initiated_holders": 2
    }, {
      'token': "0x02",
      "address": "0x3",
      "ethereum_senders": 0,
      "events_per_transaction": 0,
      "token_receivers": 0,
      "initiated_holders": 0
    }])
    self.assertCountEqual(list(result_ids), [
      ("0x01", "0x1"),
      ("0x01", "0x2"),
      ("0x02", "0x2"),
      ("0x02", "0x3"),
    ])

  def test_load_model(self):
    model = DecisionTreeClassifier()
    joblib.dump(model, 'test_model.pkl')

    loaded_model = self.multitransfers_detection._load_model('test_model')

    assert type(loaded_model) == DecisionTreeClassifier

  def test_get_prediction(self):
    test_predictions = [{
      "type": "ico",
      "probability": 0.5
    }, {
      "type": "other",
      "probability": 0.7
    }, {
      "type": "airdrop",
      "probability": 0.8
    }]
    test_objects = [[]] * 4
    self.predict_proba = MagicMock(return_value=[
      [0.1, 0.5, 0.3],
      [0.1, 0.5, 0.7],
      [0.8, 0.5, 0.7],
      [0.1, 0.2, 0.3],
    ])
    self.classes_ = ["airdrop", "ico", "other"]

    predictions = self.multitransfers_detection._get_predictions(self, test_objects, threshold=0.5)

    self.predict_proba.assert_called_with(test_objects)
    self.assertSequenceEqual(predictions, test_predictions)

  def test_save_classes(self):
    test_multitransfers = [{
      "type": "ico",
      "model": "test_model",
      "probability": 0.3,
      "address": "0x1",
      "token": "0x2"
    }]
    test_multitransfers_copy = [transfer.copy() for transfer in test_multitransfers]
    for transfer in test_multitransfers_copy:
      del transfer["model"]
    test_fields = list(test_multitransfers[0].keys())
    test_multitransfer_ids = [transfer["token"] + "." + transfer["address"] for transfer in test_multitransfers]

    self.multitransfers_detection._save_classes(test_multitransfers_copy, model_name="test_model")

    multitransfers = self.client.search(index=TEST_MULTITRANSFERS_INDEX, fields=test_fields)
    self.assertSequenceEqual(
      [transfer["_source"] for transfer in multitransfers],
      test_multitransfers
    )
    self.assertSequenceEqual(
      [transfer["_id"] for transfer in multitransfers],
      test_multitransfer_ids
    )

  def test_extract_multitransfers(self):
    test_tokens = [["0x01", "0x02"]]
    test_features = "features"
    test_predictions = "predictions"
    test_model = "model"
    mockify(self.multitransfers_detection, {
      '_iterate_tokens': MagicMock(return_value=test_tokens),
      '_get_features': MagicMock(return_value=test_features),
      '_get_predictions': MagicMock(return_value=test_predictions),
      '_load_model': MagicMock(return_value=test_model),
    }, 'extract_multitransfers')
    process = Mock(
      iterate=self.multitransfers_detection._iterate_tokens,
      get_features=self.multitransfers_detection._get_features,
      get_predictions=self.multitransfers_detection._get_predictions,
      load_model=self.multitransfers_detection._load_model,
      save_classes=self.multitransfers_detection._save_classes
    )

    self.multitransfers_detection.extract_multitransfers()

    calls = [call.load_model(), call.iterate()]
    for token_list in test_tokens:
      calls += [
        call.get_features(token_list),
        call.get_predictions(test_model, test_features),
        call.save_classes(test_predictions)
      ]
    process.assert_has_calls(calls)

  def xtest_process(self):
    test_tokens = [
      "0xe5dada80aa6477e85d09747f2842f7993d0df71c", # Dock drop and ICO
      "0xf3e014fe81267870624132ef3a646b8e83853a96", # VinChain Drops
      "0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0" # EOS ICO
    ]
    test_tokens_string = str(test_tokens)[1:-1]
    test_client = Client('localhost', 9001)
    test_contracts = test_client.execute("""
      SELECT id, address FROM ethereum_contract WHERE address in({})
    """.format(test_tokens_string))
    test_transactions = test_client.execute("""
      SELECT id, from, to, value FROM ethereum_internal_transaction WHERE to in({}) 
    """.format(test_tokens_string))
    test_transactions += test_client.execute("""
      SELECT from, to, value FROM ethereum_internal_transaction 
      WHERE from in(
        SELECT concat('0x', substring(topics[3], 27, 40))
        FROM ethereum_event
        WHERE topics[1] = '{}'
        AND address in({})
      ) 
      AND value > 0
      SAMPLE 100000
    """.format(self.test_transfer_event_hex, test_tokens_string))
    test_events = test_client.execute("""
      SELECT id, address, topics, blockNumber
      FROM ethereum_event 
      WHERE address in({})
    """.format(test_tokens_string))

    test_contracts_df = pd.DataFrame(test_contracts, columns=["id", "address"])
    test_events_df = pd.DataFrame(test_events, columns=["id", "address", "topics", "blockNumber"])
    test_transactions_df = pd.DataFrame(test_transactions, columns=["id", "from", "to", "value"])

    self.predict_proba = MagicMock(side_effect=lambda: np.random.rand(3))
    self.classes_ = ["airdrop", "ico", "other"]
    self.multitransfers_detection._load_model = MagicMock(return_value=self)

    self.client.bulk_index(index=TEST_CONTRACTS_INDEX, docs=test_contracts)
    self.client.bulk_index(index=TEST_TRANSACTIONS_INDEX, docs=test_transactions)
    self.client.bulk_index(index=TEST_EVENTS_INDEX, docs=test_events)

    self.multitransfers_detection.extract_multitransfers()

    assert self.client.count(index=TEST_MULTITRANSFERS_INDEX) > 0

TEST_MULTITRANSFERS_INDEX = "test_multitransfers"
TEST_CONTRACTS_INDEX = "test_contracts"
TEST_TRANSACTIONS_INDEX = "test_transactions"
TEST_EVENTS_INDEX = "test_events"