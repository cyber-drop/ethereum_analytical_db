from custom_elastic_search import CustomElasticSearch
from config import INDICES

class ContractTransactions:
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
      docs = [self._extract_contract_from_transactions(transaction) for transaction in contract_transactions]
      self.client.bulk_index(docs=docs, doc_type='contract', index=self.indices["contract"], refresh=True)
      self._save_contract_created(contract_transactions)

  def _extract_contract_from_transactions(self):
    raise Exception

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