from config import INDICES
from clients.custom_clickhouse import CustomClickhouse
from schema.schema import SCHEMA

STRING_PROPERTIES = {
    "tx": ["from", "hash", "blockTimestamp", "creates", "to"],
    "itx": [
        "from", "hash",
        "blockTimestamp", "callType",
        "gas", "gasUsed",
        "callType", "blockHash", "transactionHash",
        "refundAddress", "to",
        "type", "address", "balance", "blockNumber"
    ]
}

OBJECT_PROPERTIES = {
    "tx": ["decoded_input"],
    "itx": ["decoded_input", "traceAddress"]
}

TEXT_PROPERTIES = {
    "tx": ["input"],
    "itx": ["code", "input", "init", "error", "output"]
}

FAST_INDICES = {
    "internal_transaction": "itx"
}

INDEX_FIELDS = SCHEMA

PRIMARY_KEYS = {
    "block_flag": ["id", "name"],
    "contract_block": ["id", "name"]
}


class ClickhouseIndices:
    def __init__(self, indices=INDICES):
        self.client = CustomClickhouse()
        self.indices = indices

    def _create_index(self, index, fields={}, primary_key=["id"]):
        fields["id"] = "String"
        fields_string = ", ".join(["{} {}".format(name, type) for name, type in fields.items()])
        primary_key_string = ",".join(primary_key)
        create_sql = """
            CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = ReplacingMergeTree() ORDER BY ({})
        """.format(index, fields_string, primary_key_string)
        self.client.send_sql_request(create_sql)

    def prepare_indices(self):
        for key, index in self.indices.items():
            if key in INDEX_FIELDS:
                self._create_index(index, INDEX_FIELDS[key], PRIMARY_KEYS.get(key, ["id"]))
