from actions.query import Query

INCOME_SQL = """
    SELECT to AS address, sum(value) AS income
    FROM {}
    WHERE type != 'reward'
    AND address IN({})
    GROUP BY address
"""

OUTCOME_SQL = """
    SELECT from AS address, sum(value) AS outcome
    FROM {}
    WHERE type != 'reward'
    AND address IN({})
    GROUP BY address
"""

REWARD_SQL = """
    SELECT author AS address, sum(value) AS reward
    FROM {}
    WHERE type = 'reward'
    AND address IN ({})
    GROUP BY author
"""

FEE_SQL = """
    SELECT from AS address, sum(gasPrice * gasUsed) AS fee
    FROM {}
    WHERE address IN({})
    GROUP BY from
"""

FEE_REWARD_SQL = """
    SELECT address, sum(fee) AS fee_reward
    FROM (
        SELECT blockNumber, sum(gasPrice * gasUsed) AS fee
        FROM {0}
        GROUP BY blockNumber
    )
    ANY INNER JOIN (
        SELECT author AS address, blockNumber
        FROM {0}
        WHERE type = 'reward'
        AND rewardType = 'block'
        AND address IN({1})
    )
    USING blockNumber
    GROUP BY address
"""

class Balances(Query):
    def get_income(self, addresses):
        return self._send_sql_request(addresses, INCOME_SQL)

    def get_outcome(self, addresses):
        return self._send_sql_request(addresses, OUTCOME_SQL)

    def get_reward(self, addresses):
        return self._send_sql_request(addresses, REWARD_SQL)

    def get_fee(self, addresses):
        return self._send_sql_request(addresses, FEE_SQL)

    def get_fee_reward(self, addresses):
        return self._send_sql_request(addresses, FEE_REWARD_SQL)

    def get_balances(self, addresses):
        income = self.get_income(addresses)
        outcome = self.get_outcome(addresses)
        reward = self.get_reward(addresses)
        fee = self.get_fee(addresses)
        fee_reward = self.get_fee_reward(addresses)
        return {
            address: max(
                income.get(address, 0) 
                - outcome.get(address, 0) 
                + reward.get(address, 0) 
                - fee.get(address, 0) 
                + fee_reward.get(address, 0)
            , 0)
            for address in addresses
        }
