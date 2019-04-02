from config import INDICES
from clients.custom_clickhouse import CustomClickhouse
import utils

CONVERSION_EVENT = "0x276856b36cbc45526a0ba64f44611557a2a8b68662c5388e9fe6d72e86e1c8cb"


class ClickhouseBancorTrades:
    def __init__(self, indices=INDICES):
        self.indices = indices
        self.client = CustomClickhouse()

    def extract_trades(self):
        return_raw_sql = utils.generate_sql_for_value("return_raw")
        amount_raw_sql = utils.generate_sql_for_value("amount_raw")
        self.client.send_sql_request("""
        CREATE VIEW {trades_index}
        AS (
            SELECT id, from_token, to_token, trader, amount, return, buyer
            FROM (
                SELECT
                    id,
                    from_token,
                    to_token,
                    trader,
                    amount,
                    substring(data, 65, 66) AS return_raw,
                    {return_raw_sql},
                    return_raw_value AS return,
                    transactionHash
                FROM (
                    SELECT
                        id,
                        concat('0x', substring(topics[2], 27)) AS from_token,
                        concat('0x', substring(topics[3], 27)) AS to_token,
                        concat('0x', substring(topics[4], 27)) AS trader,
                        data,
                        substring(data, 3, 64) AS amount_raw,
                        {amount_raw_sql},
                        amount_raw_value AS amount,
                        transactionHash
                    FROM (
                        SELECT *
                        FROM {events_index}
                        WHERE topics[1] = '{conversion_event}'
                        AND address IN(
                            SELECT address
                            FROM {contracts_index}
                            WHERE standard_bancor_converter = 1
                        )
                    )
                    ANY LEFT JOIN (
                        SELECT id AS from_token, decimals
                        FROM {tokens_index}
                    )
                    USING from_token
                )
                ANY LEFT JOIN (
                    SELECT id AS to_token, decimals
                    FROM {tokens_index}
                )
                USING to_token
            )
            ANY LEFT JOIN (
                SELECT transactionHash, from AS buyer
                FROM {transactions_index}
                WHERE to in(
                    SELECT address
                    FROM {contracts_index}
                    WHERE standard_bancor_converter = 1
                )
            )
            USING transactionHash
        )
        """.format(
            trades_index=self.indices["bancor_trade"],
            events_index=self.indices["event"],
            tokens_index=self.indices["contract_description"],
            contracts_index=self.indices["contract"],
            transactions_index=self.indices["internal_transaction"],
            conversion_event=CONVERSION_EVENT,
            amount_raw_sql=amount_raw_sql,
            return_raw_sql=return_raw_sql
        ))

