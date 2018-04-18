from custom_elastic_search import CustomElasticSearch

class ContractTransactions:
  def __init__(self, elasticsearch_index, elasticsearch_host="http://localhost:9200", ethereum_api_host="http://localhost:8545"):
    self.index = elasticsearch_index
    self.client = CustomElasticSearch(elasticsearch_host)
    self.ethereum_api_host = ethereum_api_host

  def _iterate_contract_transactions(self):
    return self.client.iterate(self.index, 'tx', 'input:0x?*', paginate=True, scrolling=False)

  def _extract_contract_addresses(self):
    for contract_transactions in self._iterate_contract_transactions():
      contracts = [transaction["_source"]["to"] for transaction in contract_transactions]
      docs = [{'address': contract, 'id': contract} for contract in contracts]
      self.client.bulk_index(docs=docs, doc_type='contract', index=self.index, refresh=True)

  def _iterate_contracts(self):
    return self.client.iterate(self.index, 'contract', 'address:*', paginate=True, scrolling=False)

  def _detect_transactions_by_contracts(self, contracts):
    query = {
      "terms": {
        "to": contracts
      }
    }
    self.client.update_by_query(self.index, 'tx', query, "ctx._source.to_contract = true")

  def detect_contract_transactions(self):
    self._extract_contract_addresses()
    for contracts in self._iterate_contracts():
      contracts = [contract["_source"]["address"] for contract in contracts]
      self._detect_transactions_by_contracts(contracts)
