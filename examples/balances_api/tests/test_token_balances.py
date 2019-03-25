import unittest
from actions.token_balances import TokenBalances
from clickhouse_driver import Client
from unittest.mock import MagicMock

TEST_TABLE = "test_eth_token_transaction"

class TokenBalancesTestCase(unittest.TestCase):
    def setUp(self):
        self.balances = TokenBalances(TEST_TABLE)
        self.client = Client('localhost')
        self.client.execute("""
            DROP TABLE IF EXISTS {}
        """.format(TEST_TABLE))

        self.client.execute("""
            CREATE TABLE {}
            (
                id String, 
                from String, 
                to String, 
                token String,
                value Float64
            )
            ENGINE = MergeTree()
            ORDER BY id
        """.format(TEST_TABLE))

    transactions = [{
        "id": "1",
        "from": "0x1",
        "to": "0x2",
        "token": "0x01",
        "value": 100
    }, {
        "id": "2",
        "from": "0x2",
        "to": "0x1",
        "token": "0x01",
        "value": 500
    }, {
        "id": "3",
        "from": "0x1",
        "to": "0x2",
        "token": "0x02",
        "value": 100
    }, {
        "id": "4",
        "from": "0x4",
        "to": "0x3",
        "token": "0x01",
        "value": 100
    }]

    def test_get_income(self):
        test_transactions = self.transactions
        self.client.execute("""
            INSERT INTO {}
            (id, to, token, value)
            VALUES
        """.format(TEST_TABLE), test_transactions)

        result = self.balances.get_income(["0x1", "0x2"], "0x01")

        self.assertSequenceEqual(result, {
            "0x2": 100,
            "0x1": 500
        })

    def test_get_outcome(self):
        test_transactions = self.transactions
        self.client.execute("""
            INSERT INTO {}
            (id, from, token, value)
            VALUES
        """.format(TEST_TABLE), test_transactions)

        result = self.balances.get_outcome(["0x1", "0x2"], "0x01")

        self.assertSequenceEqual(result, {
            "0x2": 500,
            "0x1": 100
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
        balances = self.balances.get_balances(["0x1", "0x2", "0x3"], "0x01")

        self.assertSequenceEqual(balances, {
            "0x1": 0,
            "0x2": 2,
            "0x3": 0
        })