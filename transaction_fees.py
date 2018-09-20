from custom_elastic_search import CustomElasticSearch
from config import INDICES, PARITY_HOSTS
from web3 import Web3, HTTPProvider
import json
from multiprocessing import Pool
import utils

NUMBER_OF_PROCESSES = 10

def _extract_gas_used_sync(hashes, parity_host):
  w3 = Web3(HTTPProvider(parity_host))
  return [w3.eth.getTransactionReceipt(hash).gasUsed for hash in hashes]

class TransactionFees:
  def __init__(self, indices=INDICES, elasticsearch_host="http://localhost:9200", parity_host=PARITY_HOSTS[0][-1]):
    self.client = CustomElasticSearch(elasticsearch_host)
    self.indices = indices
    self.w3 = Web3(HTTPProvider(parity_host))
    self.pool = Pool(processes=NUMBER_OF_PROCESSES)

  def _iterate_blocks(self):
    return self.client.iterate(index=self.indices["block"], doc_type="b", query="!(_exists_:transactionFees)")

  def _extract_transactions_for_block(self, block_number):
    transactions = self.w3.eth.getBlock(block_number, True).transactions
    transactions = [dict(transaction) for transaction in transactions]
    for transaction in transactions:
      transaction["hash"] = transaction["hash"].hex()
      transaction["gasPrice"] = Web3.fromWei(transaction["gasPrice"], 'ether')
    return transactions

  def _extract_gas_used(self, hashes):
    chunks = utils.split_on_chunks(hashes, NUMBER_OF_PROCESSES)
    gas_used = self.pool.map(_extract_gas_used_sync, chunks)
    return dict([gas for gas_used_chunk in gas_used for gas in gas_used_chunk.items()])

  def _update_transactions(self, transactions):
    operations = [self.client.update_op({
      "gasPrice": transaction["gasPrice"],
      "gasUsed": transaction["gasUsed"]
    }, id=transaction["hash"] + ".0") for transaction in transactions]
    if operations:
      self.client.bulk(operations, index=self.indices["internal_transaction"], doc_type="itx", refresh=True)

  def _count_transaction_fees(self, transactions, blocks):
    transaction_fees = {}
    for transaction in transactions:
      transaction_fees[transaction["blockNumber"]] = transaction_fees.get(transaction["blockNumber"], 0) + transaction["gasPrice"] * transaction["gasUsed"]
    for block in blocks:
      transaction_fees[block] = transaction_fees.get(block, 0)
    return transaction_fees

  def _update_blocks(self, transaction_fees):
    operations = [self.client.update_op({
      "transactionFees": fee,
    }, id=block) for block, fee in transaction_fees.items()]
    if operations:
      self.client.bulk(operations, index=self.indices["block"], doc_type="b", refresh=True)

  def extract_transaction_fees(self):
    for blocks in self._iterate_blocks():
      transactions = []
      for block in blocks:
        transactions += self._extract_transactions_for_block(block["_source"]["number"])
      gas_used = self._extract_gas_used([transaction["hash"] for transaction in transactions])
      for transaction in transactions:
        transaction["gasUsed"] = gas_used[transaction["hash"]]
      self._update_transactions(transactions)
      transaction_fees = self._count_transaction_fees(transactions, [block["_source"]["number"] for block in blocks])
      self._update_blocks(transaction_fees)

