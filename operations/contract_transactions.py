from clients.custom_elastic_search import CustomElasticSearch
from clients.custom_clickhouse import CustomClickhouse
from config import INDICES
import utils

class ElasticSearchContractTransactions(utils.ContractTransactionsIterator):
  """
  Treat detect-contract and detect-contract-transaction operations
  """
  block_prefix = "transactions_detected"
  index = "internal_transaction"
  doc_type = "itx"
  contract_transactions_query = "type:create"

  def __init__(self, indices=INDICES, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.indices = indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.ethereum_api_host = ethereum_api_host

  def _iterate_contract_transactions(self):
    """
    Iterate over transactions that create contracts

    Returns
    -------
    generator
        Generator that iterates through transactions in ElasticSearch
    """
    return self.client.iterate(
      self.indices[self.index],
      self.doc_type,
      self.contract_transactions_query + ' AND !(_exists_:error) AND !(_exists_:contract_created)'
    )

  def _save_contract_created(self, transactions):
    """
    Save contract_created flag for all the processed transactions in ElasticSearch

    Parameters
    ----------
    transactions : list
        Transactions to process
    """
    self.client.update_by_query(
      index=self.indices[self.index],
      doc_type=self.doc_type,
      query={
        "ids": {
          "values": [transaction["_id"] for transaction in transactions],
        }
      },
      script="ctx._source.contract_created = true"
    )

  def extract_contract_addresses(self):
    """
    Extract contracts from transactions to ElasticSearch

    This function is an entry point for detect-contracts operation
    """
    for contract_transactions in self._iterate_contract_transactions():
      docs = [self._extract_contract_from_transactions(transaction) for transaction in contract_transactions if "error" not in transaction["_source"].keys()]
      self.client.bulk_index(docs=docs, doc_type='contract', index=self.indices["contract"], refresh=True)
      self._save_contract_created(contract_transactions)

  def _extract_contract_from_transactions(self, transaction):
    """
    Abstract method to extract contract information from transaction

    Parameters
    ----------
    transaction : dict
        Transaction with contract info in ElasticSearch JSON format, i.e.
        {"_id": TRANSACTION_ID, "_source": {"document": "fields"}}
    """
    transaction_body = transaction["_source"]
    return {
      "id": transaction_body["address"],
      "address": transaction_body["address"],
      "owner": transaction_body["from"],
      "bytecode": transaction_body["code"],
      "blockNumber": transaction_body["blockNumber"],
      "parent_transaction": transaction["_id"]
    }

  def _iterate_contracts_without_detected_transactions(self, max_block):
    """
    Iterate over contracts with undetected transactions before specified block

    Parameters
    ----------
    max_block : int
        Block limit
    """
    query = {
      "query_string": {
        "query": 'address:*'
      }
    }
    return self._iterate_contracts(max_block, query)

  def _detect_transactions_by_contracts(self, contracts, max_block):
    """
    Save to_contract flag in ElasticSearch for transactions before specified block to specified contracts

    Parameters
    ----------
    contracts : list

    max_block : int
        Block limit
    """
    transactions_query = {
      "bool": {
        "must": [
          {"terms": {"to": [contract["_source"]["address"] for contract in contracts]}},
          self._create_transactions_request(contracts, max_block)
        ]
      }

    }
    self.client.update_by_query(self.indices[self.index], self.doc_type, transactions_query,
                                "ctx._source.to_contract = true")

  def detect_contract_transactions(self):
    """
    Detect transactions to contracts in ElasticSearch

    This function is an entry point for detect-contract-transactions operation
    """
    max_block = utils.get_max_block()
    for contracts in self._iterate_contracts_without_detected_transactions(max_block):
      self._detect_transactions_by_contracts(contracts, max_block)
      self._save_max_block([contract["_source"]["address"] for contract in contracts], max_block)

class ClickhouseContractTransactions:
  def __init__(self, indices=INDICES):
    self.indices = indices
    self.client = CustomClickhouse()

  def _get_fields(self):
    fields = {
      "id": "coalesce(address, id)",
      "blockNumber": "blockNumber",
      "address": "address",
      "owner": "from",
      "bytecode": "code"
    }
    fields_string = ", ".join([
      "{} AS {}".format(field, alias)
      for alias, field in fields.items()
    ])
    return fields_string

  def extract_contract_addresses(self):
    fields_string = self._get_fields()
    engine_string = 'ENGINE = ReplacingMergeTree() ORDER BY id'
    condition = "type = 'create' AND error IS NULL AND parent_error IS NULL"
    sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS {} {} AS (SELECT {} FROM {} WHERE {})".format(
      self.indices["contract"],
      engine_string,
      fields_string,
      self.indices["internal_transaction"],
      condition
    )
    self.client.send_sql_request(sql)
