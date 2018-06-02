from custom_elastic_search import CustomElasticSearch
from config import INDICES

class ContractTransactions:
  def __init__(self, indices=INDICES, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.indices = indices
    self.client = CustomElasticSearch(elasticsearch_host)
    self.ethereum_api_host = ethereum_api_host

  def _iterate_contract_transactions(self):
    return self.client.iterate(self.indices[self.index], self.doc_type, self.contract_transactions_query)

  def extract_contract_addresses(self):
    for contract_transactions in self._iterate_contract_transactions():
      docs = [self._extract_contract_from_transactions(transaction["_source"]) for transaction in contract_transactions]
      self.client.bulk_index(docs=docs, doc_type='contract', index=self.indices["contract"], refresh=True)

  def _extract_contract_from_transactions(self):
    raise Exception

  def _iterate_contracts(self):
    return self.client.iterate(self.indices["contract"], 'contract', 'address:* AND !(_exists_:transactions_detected)')

  def _detect_transactions_by_contracts(self, contracts):
    transactions_query = {
      "terms": {
        "to": contracts
      }
    }
    contracts_query = {
      "terms": {
        "address": contracts
      }
    }
    self.client.update_by_query(self.indices[self.index], self.doc_type, transactions_query, "ctx._source.to_contract = true")
    self.client.update_by_query(self.indices["contract"], 'contract', contracts_query, "ctx._source.transactions_detected = true")

  def detect_contract_transactions(self):
    for contracts in self._iterate_contracts():
      contracts = [contract["_source"]["address"] for contract in contracts]
      self._detect_transactions_by_contracts(contracts)

class ExternalContractTransactions(ContractTransactions):
  index = "transaction"
  doc_type = "tx"
  contract_transactions_query = '(_exists_:creates)'

  def _extract_contract_from_transactions(self, transaction):
    return {
      "id": transaction["creates"],
      "address": transaction["creates"],
      "owner": transaction["from"],
      "parent_transaction": transaction["hash"],
      "blockNumber": transaction["blockNumber"],
      "bytecode": transaction["input"]
    }

class InternalContractTransactions(ContractTransactions):
  index = "internal_transaction"
  doc_type = "itx"
  contract_transactions_query = "type:create AND !(_exists_:error)"

  def _extract_contract_from_transactions(self, transaction):
    return {
      "id": transaction["address"],
      "address": transaction["address"],
      "creator": transaction["from"],
      "bytecode": transaction["code"]
    }