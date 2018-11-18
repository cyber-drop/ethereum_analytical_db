import unittest
from operations.multitransfers_detection import ClickhouseMultitransfersDetection
from tests.test_utils import TestClickhouse
from sklearn.tree import DecisionTreeClassifier
from sklearn.externals import joblib
from unittest.mock import MagicMock

class MultitransfersDetectionTestCase(unittest.TestCase):
  def setUp(self):
    self.indices = {
      "multitransfer": TEST_MULTITRANSFERS_INDEX,
      "contract": TEST_CONTRACTS_INDEX,
      "internal_transaction": TEST_TRANSACTIONS_INDEX
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
    test_fields = list(test_multitransfers[0].keys())
    test_multitransfer_ids = [transfer["token"] + "." + transfer["address"] for transfer in test_multitransfers]

    self.multitransfers_detection._save_classes(test_multitransfers_copy)

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
    # Iterate over top addresses
    #
    pass

TEST_MULTITRANSFERS_INDEX = "test_multitransfers"
TEST_CONTRACTS_INDEX = "test_contracts"
TEST_TRANSACTIONS_INDEX = "test_transactions"