from custom_elastic_search import CustomElasticSearch
from config import INDICES
import utils

class ContractTransactions(utils.ContractTransactionsIterator):
  block_prefix = "transactions_detected"
  def __init__(self, indices=INDICES, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.indices = indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.ethereum_api_host = ethereum_api_host

  def _iterate_contract_transactions(self):
    return self.client.iterate(
      self.indices[self.index],
      self.doc_type,
      self.contract_transactions_query + ' AND !(_exists_:error) AND !(_exists_:contract_created)'
    )

  def _save_contract_created(self, transactions):
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
    for contract_transactions in self._iterate_contract_transactions():
      docs = [self._extract_contract_from_transactions(transaction) for transaction in contract_transactions if "error" not in transaction["_source"].keys()]
      self.client.bulk_index(docs=docs, doc_type='contract', index=self.indices["contract"], refresh=True)
      self._save_contract_created(contract_transactions)

  def _extract_contract_from_transactions(self):
    raise Exception

  def _iterate_contracts_without_detected_transactions(self, max_block):
    query = {
      "query_string": {
        "query": 'address:*'
      }
    }
    return self._iterate_contracts(max_block, query)

  def _detect_transactions_by_contracts(self, contracts, max_block):
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
    max_block = utils.get_max_block()
    for contracts in self._iterate_contracts_without_detected_transactions(max_block):
      self._detect_transactions_by_contracts(contracts, max_block)
      self._save_max_block([contract["_source"]["address"] for contract in contracts], max_block)

class ExternalContractTransactions(ContractTransactions):
  index = "transaction"
  doc_type = "tx"
  contract_transactions_query = '(_exists_:creates)'

  def _extract_contract_from_transactions(self, transaction):
    transaction_body = transaction["_source"]
    return {
      "id": transaction_body["creates"],
      "address": transaction_body["creates"],
      "owner": transaction_body["from"],
      "parent_transaction": transaction["_id"],
      "blockNumber": transaction_body["blockNumber"],
      "bytecode": transaction_body["input"]
    }

class InternalContractTransactions(ContractTransactions):
  index = "internal_transaction"
  doc_type = "itx"
  contract_transactions_query = "type:create"

  def _extract_contract_from_transactions(self, transaction):
    transaction_body = transaction["_source"]
    return {
      "id": transaction_body["address"],
      "address": transaction_body["address"],
      "owner": transaction_body["from"],
      "bytecode": transaction_body["code"],
      "blockNumber": transaction_body["blockNumber"],
      "parent_transaction": transaction["_id"]
    }
