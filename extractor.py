#!/usr/bin/env python3
import click
from operations import clickhouse
from config import DATABASE

OPERATIONS = {
    "clickhouse": [
        ("prepare-database", clickhouse.prepare_indices_and_views),
        ("start", clickhouse.synchronize),
        ("start-full", clickhouse.synchronize_full),
        ("prepare-indices", clickhouse.prepare_indices),
        ("prepare-erc-transactions-view", clickhouse.extract_token_transactions),
        ("prepare-bancor-trades-view", clickhouse.prepare_bancor_trades),
        ("prepare-contracts-view", clickhouse.prepare_contracts_view),
        ("extract-blocks", clickhouse.prepare_blocks),
        ("extract-traces", clickhouse.extract_traces),
        ("extract-events", clickhouse.extract_events),
        ("extract-tokens", clickhouse.extract_tokens),
        ("download-contracts-abi", clickhouse.extract_contracts_abi),
        ("parse-transactions-inputs", clickhouse.parse_transactions_inputs),
        ("parse-events-inputs", clickhouse.parse_events_inputs),
        ("download-prices", clickhouse.extract_prices),
        ("test", clickhouse.run_tests)
    ]
}


@click.group()
def start_process():
    """
    Ethereum extractor
    """
    pass


def wrap_operations():
    for name, operation in OPERATIONS[DATABASE]:
        start_process.command(name)(operation)


wrap_operations()
if __name__ == '__main__':
    start_process()
