#!/usr/bin/env python3
import click
from operations import elasticsearch, clickhouse
from config import DATABASE

OPERATIONS = {
  "elasticsearch": {
    "prepare-indices": elasticsearch.prepare_indices
  },
  "clickhouse": {
    "prepare-indices": clickhouse.prepare_indices
  }
}

def get_operation(name):
  return OPERATIONS[DATABASE][name]

@click.command()
@click.option('--operation', help='Action to perform', default='prepare-indices')
def start_process(operation):
  get_operation(operation)()

if __name__ == '__main__':
  start_process()
