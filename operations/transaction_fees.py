from config import INDICES, PARITY_HOSTS
from web3 import Web3, HTTPProvider
from multiprocessing import Pool
import utils
from clients.custom_clickhouse import CustomClickhouse

NUMBER_OF_PROCESSES = 10

def _extract_gas_used_sync(hashes, parity_host=PARITY_HOSTS[0][-1]):
  w3 = Web3(HTTPProvider(parity_host))
  return {hash: w3.eth.getTransactionReceipt(hash).gasUsed for hash in hashes}

def _extract_transactions_for_blocks_sync(blocks, parity_host=PARITY_HOSTS[0][-1]):
  w3 = Web3(HTTPProvider(parity_host))
  result = []
  for block in blocks:
    transactions = w3.eth.getBlock(block, True).transactions
    transactions = [dict(transaction) for transaction in transactions]
    for transaction in transactions:
      transaction["hash"] = transaction["hash"].hex()
      transaction["gasPrice"] = Web3.fromWei(transaction["gasPrice"], 'ether')
    result += transactions
  return result

class TransactionFees:
  def __init__(self, indices, client, parity_host):
    self.client = client
    self.indices = indices
    self.w3 = Web3(HTTPProvider(parity_host))
    self.pool = Pool(processes=NUMBER_OF_PROCESSES)

  def _iterate_blocks(self):
    flags_sql = "SELECT id, value FROM {} FINAL WHERE name = 'fees_extracted'".format(self.indices["block_flag"])
    query = "ANY LEFT JOIN ({}) USING id WHERE value IS NULL".format(
      flags_sql
    )
    return self.client.iterate(index=self.indices["block"], query=query, fields=[])

  def _extract_transactions_for_blocks(self, blocks):
    chunks = utils.split_on_chunks(blocks, NUMBER_OF_PROCESSES)
    transactions = self.pool.map(_extract_transactions_for_blocks_sync, chunks)
    return [transaction for transactions_chunk in transactions for transaction in transactions_chunk]

  def _extract_gas_used(self, hashes):
    chunks = utils.split_on_chunks(hashes, NUMBER_OF_PROCESSES)
    gas_used = self.pool.map(_extract_gas_used_sync, chunks)
    return dict([gas for gas_used_chunk in gas_used for gas in gas_used_chunk.items()])

  def _update_transactions(self, transactions):
    transaction_fees = [{
      "id": transaction["hash"] + ".0",
      "gasPrice": transaction["gasPrice"],
      "gasUsed": transaction["gasUsed"]
    } for transaction in transactions]
    if transaction_fees:
      self.client.bulk_index(index=self.indices["transaction_fee"], docs=transaction_fees)

  def _count_transaction_fees(self, transactions, blocks):
    transaction_fees = {}
    for transaction in transactions:
      transaction_fees[transaction["blockNumber"]] = transaction_fees.get(transaction["blockNumber"], 0) + transaction["gasPrice"] * transaction["gasUsed"]
    for block in blocks:
      transaction_fees[block] = transaction_fees.get(block, 0)
    return transaction_fees

  def _update_blocks(self, blocks):
    docs = [{"id": block, "name": "fees_extracted", "value": True} for block in blocks]
    self.client.bulk_index(index=self.indices["block_flag"], docs=docs)

  def extract_transaction_fees(self):
    for blocks in self._iterate_blocks():
      block_numbers = [block["_source"]["number"] for block in blocks]
      transactions = self._extract_transactions_for_blocks(block_numbers)
      gas_used = self._extract_gas_used([transaction["hash"] for transaction in transactions])
      for transaction in transactions:
        transaction["gasUsed"] = gas_used[transaction["hash"]]
      self._update_transactions(transactions)
      self._update_blocks(block_numbers)

class ClickhouseTransactionFees(TransactionFees):
  def __init__(self, indices=INDICES, parity_host=PARITY_HOSTS[0][-1]):
    super().__init__(indices, CustomClickhouse(), parity_host)
