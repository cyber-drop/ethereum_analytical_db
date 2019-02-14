from config import INDICES
from clients.custom_clickhouse import CustomClickhouse

TRANSFER_EVENT = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


class ClickhouseTokenHolders():
    def __init__(self, indices=INDICES):
        self.indices = indices
        self.client = CustomClickhouse()

    def _generate_sql_for_data(self):
        return " + ".join([
            "reinterpretAsUInt64(reverse(unhex(substring(data, {}, 16)))) * {}".format(i + 5,
                                                                                       16 ** (64 - i - 18) / (10 ** 18))
            for i in range(0, 64, 16)
        ])

    def extract_token_transactions(self):
        value_sql = self._generate_sql_for_data()
        sql = """
      CREATE MATERIALIZED VIEW IF NOT EXISTS {index} 
      ENGINE = ReplacingMergeTree() ORDER BY id
      POPULATE
      AS 
      (
        SELECT 
          concat('0x', substring(topics[2], 27, 40)) AS from,
          concat('0x', substring(topics[3], 27, 40)) AS to,
          ({value_sql}) AS value,
          data AS value_raw,
          id,
          address AS token,
          transactionHash,
          blockNumber
        FROM {event}
        ANY INNER JOIN (
          SELECT address 
          FROM {contract} 
          WHERE standard_erc20 = 1
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
            contract=self.indices["contract"]
        )
        self.client.send_sql_request(sql)
