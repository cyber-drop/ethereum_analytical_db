SCHEMA = {
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