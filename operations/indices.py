from config import INDICES
from clients.custom_clickhouse import CustomClickhouse

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

INDEX_FIELDS = {
    "block": {
        "number": "Int64",
        "timestamp": "DateTime"
    },
    "internal_transaction": {
        "blockNumber": "Int64",
        "from": "Nullable(String)",
        "to": "Nullable(String)",
        "value": "Nullable(Float64)",
        "input": "Nullable(String)",
        "output": "Nullable(String)",
        "gas": "Nullable(String)",
        "gasUsed": "Nullable(Int32)",
        "gasPrice": "Nullable(Float64)",
        "blockHash": "String",
        "transactionHash": "Nullable(String)",
        "transactionPosition": "Nullable(Int32)",
        "subtraces": "Int32",
        "traceAddress": "Array(Int32)",
        "type": "String",
        "callType": "Nullable(String)",
        "address": "Nullable(String)",
        "code": "Nullable(String)",
        "init": "Nullable(String)",
        "refundAddress": "Nullable(String)",
        "error": "Nullable(String)",
        "parent_error": "Nullable(UInt8)",
        "balance": "Nullable(String)",
        "author": "Nullable(String)",
        "rewardType": "Nullable(String)",
        "result": "Nullable(String)"
    },
    "block_flag": {
        "name": "String",
        "value": "Nullable(UInt8)"
    },
    "contract_abi": {
        "abi_extracted": "Nullable(UInt8)",
        "abi": "Nullable(String)"
    },
    "contract_block": {
        "name": "String",
        "value": "Int64"
    },
    "event": {
        'type': 'String',
        'logIndex': 'Int32',
        'transactionLogIndex': 'Int32',
        'data': 'String',
        'transactionIndex': 'Int32',
        'address': 'String',
        'transactionHash': 'String',
        'blockHash': 'String',
        'blockNumber': 'Int32',
        'topics': 'Array(String)'
    },
    "transaction_input": {
        "name": "String",
        "params": "Nested(type String, value String)"
    },
    "event_input": {
        "name": "String",
        "params": "Nested(type String, value String)"
    },
    "price": {
        "address": "String",
        "USD": "Float64",
        "BTC": "Float64",
        "ETH": "Float64",
        "timestamp": "DateTime"
    },
    "contract_description": {
        "token_name": "Nullable(String)",
        "token_symbol": "Nullable(String)",
        "decimals": "Nullable(UInt8)",
        "total_supply": "Nullable(Int64)",
        "token_owner": "Nullable(String)",
        "cmc_id": "Nullable(String)",
        "website_slug": "Nullable(String)"
    }
}

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
