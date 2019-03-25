from actions.query import Query

INCOME_SQL = """
    SELECT to AS address, sum(value) AS income
    FROM {}
    WHERE address IN({})
    AND token = '{}'
    GROUP BY address
"""

OUTCOME_SQL = """
    SELECT from AS address, sum(value) AS outcome
    FROM {}
    WHERE address IN({})
    AND token = '{}'
    GROUP BY address
"""

class TokenBalances(Query):
    def get_income(self, addresses, token):
        return self._send_sql_request(addresses, INCOME_SQL.format("{}", "{}", token))

    def get_outcome(self, addresses, token):
        return self._send_sql_request(addresses, OUTCOME_SQL.format("{}", "{}", token))

    def get_balances(self, addresses, token):
        income = self.get_income(addresses, token)
        outcome = self.get_outcome(addresses, token)
        return {
            address: max(
                income.get(address, 0) 
                - outcome.get(address, 0) 
            , 0)
            for address in addresses
        }