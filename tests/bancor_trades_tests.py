import unittest
from tests.test_utils import TestClickhouse
from operations.bancor_trades import ClickhouseBancorTrades, CONVERSION_EVENT

EVENT_ADDRESS_LENGTH = len("0x000000000000000000000000263627126a771fa9745763495d3975614e235298")
EVENT_VALUE_LENGTH = len("0x0000000000000000000000000000000000000000000a50161174cc045b3a8000")
TRANSACTION_ADDRESS_LENGTH = len("0x5cf04716ba20127f1e2297addcf4b5035000c9eb")
TEST_EVENTS_INDEX = "test_events"
TEST_TRADES_INDEX = "test_trades"
TEST_TOKENS_INDEX = "test_tokens"
TEST_CONTRACTS_INDEX = "test_contracts"
TEST_TRANSACTIONS_INDEX = "test_transactions"

class BancorTradesTestCase(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.client = TestClickhouse()
        self.client.prepare_indices({
            "event": TEST_EVENTS_INDEX,
            "contract_description": TEST_TOKENS_INDEX,
            "contract": TEST_CONTRACTS_INDEX,
            "internal_transaction": TEST_TRANSACTIONS_INDEX
        })
        self.client.send_sql_request("DROP TABLE IF EXISTS {}".format(TEST_TRADES_INDEX))
        self.bancor_trades = ClickhouseBancorTrades({
            "event": TEST_EVENTS_INDEX,
            "bancor_trade": TEST_TRADES_INDEX,
            "contract_description": TEST_TOKENS_INDEX,
            "contract": TEST_CONTRACTS_INDEX,
            "internal_transaction": TEST_TRANSACTIONS_INDEX
        })
        self.bancor_trades.extract_trades()

    def _get_event_address(self, hex_string):
        return "{0:#0{1}x}".format(int(hex_string, 0), EVENT_ADDRESS_LENGTH)

    def _get_event_value(self, value):
        return "{0:#0{1}x}".format(value, EVENT_VALUE_LENGTH)

    def _get_transaction_address(self, hex_string):
        return "{0:#0{1}x}".format(int(hex_string, 0), TRANSACTION_ADDRESS_LENGTH)

    def _create_conversion_event(self, event, id, address="0x0", transaction="0x0"):
        return {
            "id": hex(id),
            "topics": [
                CONVERSION_EVENT,
                self._get_event_address(event["from_token"]),
                self._get_event_address(event["to_token"]),
                self._get_event_address(event.get("trader", '0x0')),
            ],
            "address": address,
            "transactionHash": transaction,
            "data":
                self._get_event_value(event.get("amount", 0)) +
                self._get_event_value(event.get("return", 0))[2:] +
                self._get_event_value(10)[2:]
        }

    def test_extract_trade_from_event(self):
        test_contracts = [{
            "id": "0x0",
            "address": "0x0",
            "standard_bancor_converter": 1
        }]
        test_trades = [{
            "from_token": "0x01",
            "to_token": "0x02",
            "trader": "0x1",
        }]
        test_events = [self._create_conversion_event(event, index) for index, event in enumerate(test_trades)]
        test_converted_trades = [{
            field: self._get_transaction_address(value)
            for field, value in trade.items()
        } for trade in test_trades]
        self.client.bulk_index(index=TEST_EVENTS_INDEX, docs=test_events)
        self.client.bulk_index(index=TEST_CONTRACTS_INDEX, docs=test_contracts)
        trades = self.client.search(index=TEST_TRADES_INDEX, fields=["from_token", "to_token", "trader"])
        trades = [trade["_source"] for trade in trades]
        self.assertCountEqual(trades, test_converted_trades)

    def test_extract_values_with_decimals(self):
        test_contracts = [{
            "id": "0x0",
            "address": "0x0",
            "standard_bancor_converter": 1
        }]
        test_tokens = [{
            "id": self._get_transaction_address("0x1"),
            "decimals": 16
        }, {
            "id": self._get_transaction_address("0x2"),
            "decimals": 0
        }]
        test_trades = [{
            "from_token": self._get_event_address("0x1"),
            "to_token": self._get_event_address("0x2"),
            "amount": 100 * 10 ** 16,
            "return": 100
        }, {
            "from_token": self._get_event_address("0x1"),
            "to_token": self._get_event_address("0x3"),
            "amount": 100 * 10 ** 16,
            "return": 100 * 10 ** 18
        }]
        test_events = [self._create_conversion_event(event, index) for index, event in enumerate(test_trades)]
        self.client.bulk_index(index=TEST_TOKENS_INDEX, docs=test_tokens)
        self.client.bulk_index(index=TEST_EVENTS_INDEX, docs=test_events)
        self.client.bulk_index(index=TEST_CONTRACTS_INDEX, docs=test_contracts)
        trades = self.client.search(index=TEST_TRADES_INDEX, fields=["from_token", "to_token", "amount", "return"])
        trades = [trade["_source"] for trade in trades]
        self.assertCountEqual(trades, [{
            "from_token": self._get_transaction_address("0x1"),
            "to_token": self._get_transaction_address("0x2"),
            "amount": 100,
            "return": 100
        }, {
            "from_token": self._get_transaction_address("0x1"),
            "to_token": self._get_transaction_address("0x3"),
            "amount": 100,
            "return": 100
        }])

    def test_extract_trades_only_for_bancor_standard(self):
        test_contracts = [{
            "id": "0x1",
            "address": "0x1",
            "standard_bancor_converter": 1
        }, {
            "id": "0x2",
            "address": "0x2",
            "standard_bancor_converter": 0
        }]
        test_events = [
            self._create_conversion_event({
                "from_token": "0x1",
                "to_token": "0x1",
                "trader": "0x1",
                "amount": 1,
                "return": 1
            }, 0, address="0x1"), {
                "id": "0x1",
                "topics": [CONVERSION_EVENT],
                "transactionHash": "0x0",
                "address": "0x2",
                "data": "0x"
            }
        ]
        self.client.bulk_index(index=TEST_CONTRACTS_INDEX, docs=test_contracts)
        self.client.bulk_index(index=TEST_EVENTS_INDEX, docs=test_events)

        trades = self.client.search(index=TEST_TRADES_INDEX, fields=[])
        trades = [trade["_id"] for trade in trades]
        self.assertCountEqual(trades, ["0x0"])

    def test_extract_trade_buyer(self):
        test_transasctions = [{
            "id": "0x1",
            "transactionHash": "0x1",
            "from": "0x1",
            "to": "0x0"
        }]
        test_contracts = [{
            "id": "0x0",
            "address": "0x0",
            "standard_bancor_converter": 1
        }]
        test_events = [
            self._create_conversion_event({
                "from_token": "0x1",
                "to_token": "0x1",
            }, 0, transaction="0x1"),
            self._create_conversion_event({
                "from_token": "0x1",
                "to_token": "0x1",
            }, 0, transaction="0x2"),
        ]
        self.client.bulk_index(index=TEST_CONTRACTS_INDEX, docs=test_contracts)
        self.client.bulk_index(index=TEST_EVENTS_INDEX, docs=test_events)
        self.client.bulk_index(index=TEST_TRANSACTIONS_INDEX, docs=test_transasctions)

        trades = self.client.search(index=TEST_TRADES_INDEX, fields=["buyer"])
        trades = [trade["_source"]["buyer"] for trade in trades]
        self.assertCountEqual(trades, ["0x1", None])
