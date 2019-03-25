import unittest
from actions.balances import Balances
from clickhouse_driver import Client
from unittest.mock import MagicMock

TEST_TABLE = "test_eth_transaction"

class BalancesTestCase(unittest.TestCase):
    def setUp(self):
        self.balances = Balances(TEST_TABLE)
        self.client = Client('localhost')
        self.client.execute("""
            DROP TABLE IF EXISTS {}
        """.format(TEST_TABLE))

        self.client.execute("""
            CREATE TABLE {}
            (
                id String, 
                from Nullable(String), 
                to Nullable(String), 
                author Nullable(String), 
                gasUsed Int64, 
                gasPrice Float64, 
                type String, 
                rewardType Nullable(String),
                blockNumber Int64,
                value Float64
            )
            ENGINE = MergeTree()
            ORDER BY id
        """.format(TEST_TABLE))

    transactions = [{
        "id": "1",
        "from": "0x1",
        "to": "0x2",
        "author": None,
        "type": "call",
        "rewardType": None,
        "gasUsed": 10000,
        "gasPrice": 0.01,
        "blockNumber": 1,
        "value": 100
    }, {
        "id": "2",
        "from": "0x2",
        "to": "0x1",
        "author": None,
        "type": "call",
        "rewardType": None,
        "gasUsed": 20000,
        "gasPrice": 0.02,
        "blockNumber": 1,
        "value": 50
    }, {
        "id": "3",
        "from": "0x4",
        "to": "0x3",
        "author": None,
        "type": "call",
        "rewardType": None,
        "gasUsed": 30000,
        "gasPrice": 0.03,
        "blockNumber": 1,
        "value": 10
    }, {
        "id": "4",
        "from": None,
        "to": None,
        "author": "0x1",
        "type": "reward",
        "rewardType": "block",
        "gasUsed": 0,
        "gasPrice": 0,
        "blockNumber": 1,
        "value": 1
    }, {
        "id": "4",
        "from": None,
        "to": None,
        "author": "0x2",
        "type": "reward",
        "rewardType": "uncle",
        "gasUsed": 0,
        "gasPrice": 0,
        "blockNumber": 1,
        "value": 0.1
    }]

    def test_get_income(self):
        test_transactions = self.transactions
        self.client.execute("""
            INSERT INTO {}
            (id, to, type, value)
            VALUES
        """.format(TEST_TABLE), test_transactions)

        result = self.balances.get_income(["0x1", "0x2"])

        self.assertSequenceEqual(result, {
            "0x2": 100,
            "0x1": 50
        })

    def test_get_outcome(self):
        test_transactions = self.transactions
        self.client.execute("""
            INSERT INTO {}
            (id, from, type, value)
            VALUES
        """.format(TEST_TABLE), test_transactions)

        result = self.balances.get_outcome(["0x1", "0x2"])

        self.assertSequenceEqual(result, {
            "0x2": 50,
            "0x1": 100
        })

    def test_get_reward(self):
        test_transactions = self.transactions
        self.client.execute("""
            INSERT INTO {}
            (id, author, type, value)
            VALUES
        """.format(TEST_TABLE), test_transactions)

        result = self.balances.get_reward(["0x1", "0x2"])

        self.assertSequenceEqual(result, {
            "0x1": 1,
            "0x2": 0.1
        })

    def test_get_fee(self):
        test_transactions = self.transactions
        self.client.execute("""
            INSERT INTO {}
            (id, from, gasUsed, gasPrice)
            VALUES
        """.format(TEST_TABLE), test_transactions)

        result = self.balances.get_fee(["0x1", "0x2"])

        self.assertSequenceEqual(result, {
            "0x1": 10000 * 0.01,
            "0x2": 20000 * 0.02
        })

    def test_get_fee_reward(self):
        test_transactions = self.transactions
        self.client.execute("""
            INSERT INTO {}
            (id, blockNumber, author, gasUsed, gasPrice, type, rewardType)
            VALUES
        """.format(TEST_TABLE), test_transactions)

        result = self.balances.get_fee_reward(["0x1", "0x2"])

        self.assertSequenceEqual(result, {
            "0x1": 10000 * 0.01 + 20000 * 0.02 + 30000 * 0.03
        })

    def test_get_balances(self):
        self.balances.get_income = MagicMock(return_value={
            "0x1": 1,
            "0x2": 2
        })
        self.balances.get_outcome = MagicMock(return_value={
            "0x1": 2,
            "0x3": 10
        })
        self.balances.get_reward = MagicMock(return_value={
            "0x1": 10,
            "0x4": 1
        })
        self.balances.get_fee = MagicMock(return_value={
            "0x1": 1,
            "0x3": 10
        })
        self.balances.get_fee_reward = MagicMock(return_value={
            "0x1": 11,
        })
        balances = self.balances.get_balances(["0x1", "0x2", "0x3"])
        print(balances)

        self.assertSequenceEqual(balances, {
            "0x1": 1 - 2 + 10 - 1 + 11,
            "0x2": 2,
            "0x3": 0
        })