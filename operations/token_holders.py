from config import INDICES
from clients.custom_clickhouse import CustomClickhouse
import utils

TRANSFER_EVENT = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


class ClickhouseTokenHolders():
    def __init__(self, indices=INDICES):
        self.indices = indices
        self.client = CustomClickhouse()

    def extract_token_transactions(self):
        """
        Creates materialized view with token transactions extracted from Transfer events

        This function is an entry point for prepare-erc-transactions-view operation
        """
        value_sql = utils.generate_sql_for_value("data")
        sql = """
      CREATE MATERIALIZED VIEW IF NOT EXISTS {index} 
      ENGINE = ReplacingMergeTree() ORDER BY id
      POPULATE
      AS 
      (
        SELECT 
          concat('0x', substring(topics[2], 27, 40)) AS from,
          concat('0x', substring(topics[3], 27, 40)) AS to,
          {value_sql},
          data_value AS value,
          id,
          address AS token,
          transactionHash,
          blockNumber
        FROM {event}
        ANY INNER JOIN (
          SELECT id AS address, decimals
          FROM {contract} 
        )
        USING address
        WHERE
          topics[1] = '{transfer_topic}'
      )
    """.format(
            index=self.indices["token_transaction"],
            value_sql=value_sql,
            transfer_topic=TRANSFER_EVENT,
            event=self.indices["event"],
            contract=self.indices["contract_description"],
        )
        self.client.send_sql_request(sql)
