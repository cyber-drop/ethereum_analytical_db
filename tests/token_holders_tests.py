import unittest
from token_holders import TokenHolders, ExternalTokenTransactions, InternalTokenTransactions
from tests.test_utils import TestElasticSearch
import time

class TokenHoldersTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_INDEX)
    self.client.recreate_index(TEST_TX_INDEX)
    self.client.recreate_index(TEST_TOKEN_TX_INDEX)
    self.client.recreate_index(TEST_ITX_INDEX)
    self.token_holders = self.token_holders_class({'contract': TEST_INDEX, 'internal_transaction': TEST_ITX_INDEX, 'transaction': TEST_TX_INDEX, 'token_tx': TEST_TOKEN_TX_INDEX})

class ExternalTokenTransactionsTestCase(TokenHoldersTestCase, unittest.TestCase):
  token_holders_class = ExternalTokenTransactions

  def iterate_processed(self):
    return self.token_holders.client.iterate(TEST_INDEX, 'contract', '_exists_:cmc_id AND tx_descr_scanned:true')

  def iterate_supply_transfers(self):
    return self.token_holders.client.iterate(TEST_TOKEN_TX_INDEX, 'tx', 'method:initial')

  def test_extract_token_txs(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[0], 'total_supply': '100000000', 'blockNumber': 5000000, 'owner': '0x1554aa0026292d03cfc8a2769df8dd4d169d590a', 'parent_transaction': TEST_PARENT_TXS[0], 'cmc_id': '1234', 'token_name': TEST_TOKEN_NAMES[0], 'token_symbol': TEST_TOKEN_SYMBOLS[0], 'abi': ['mock_abi'], 'decimals': 18}, id=TEST_TOKEN_ADDRESSES[0], refresh=True)
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_TX_INDEX, 'tx', tx, refresh=True)
    self.token_holders._extract_tokens_txs(['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d'])
    
    token_txs = self.token_holders._iterate_token_tx_descriptions('0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d')
    token_txs = [tx for txs_list in token_txs for tx in txs_list]
    methods = [tx['_source']['method'] for tx in token_txs]
    amounts = [tx['_source']['raw_value'] for tx in token_txs]
    with_error = [tx for tx in token_txs if tx['_source']['valid'] == False]
    self.assertCountEqual(['transfer', 'approve', 'transferFrom'], methods)
    self.assertCountEqual(['356245680000000000000', '356245680000000000000', '2266000000000000000000'], amounts)
    assert len(with_error) == 1

  def test_iterate_unprocessed_tokens(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[0], 'total_supply': '100000000', 'blockNumber': 5000000, 'owner': '0x1554aa0026292d03cfc8a2769df8dd4d169d590a', 'parent_transaction': TEST_PARENT_TXS[0], 'cmc_id': '1234', 'token_name': TEST_TOKEN_NAMES[0], 'token_symbol': TEST_TOKEN_SYMBOLS[0], 'abi': ['mock_abi'], 'decimals': 18}, id=TEST_TOKEN_ADDRESSES[0], refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[1], 'total_supply': '100000000', 'blockNumber': 5000000, 'owner': '0x1554aa0026292d03cfc8a2769df8dd4d169d590a', 'parent_transaction': TEST_PARENT_TXS[0], 'cmc_id': '1235', 'tx_descr_scanned': True, 'token_name': TEST_TOKEN_NAMES[0], 'token_symbol': TEST_TOKEN_SYMBOLS[0], 'abi': ['mock_abi'], 'decimals': 18}, id=TEST_TOKEN_ADDRESSES[1], refresh=True)
    tokens = self.token_holders._iterate_tokens()
    tokens = [t['_source'] for token in tokens for t in token]
    assert tokens[0]['cmc_id'] == '1234'

  def test_get_listed_tokens_txs(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'total_supply': '100000000', 'blockNumber': 5000000, 'owner': '0x1554aa0026292d03cfc8a2769df8dd4d169d590a', 'parent_transaction': TEST_PARENT_TXS[0], 'cmc_id': str(1234+i), 'token_name': TEST_TOKEN_NAMES[i], 'token_symbol': TEST_TOKEN_SYMBOLS[i], 'abi': ['mock_abi'], 'decimals': 18}, id=address, refresh=True)
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_TX_INDEX, 'tx', tx, refresh=True)
    self.token_holders.get_listed_tokens_txs()
    
    all_descrptions = self.token_holders._iterate_tx_descriptions()
    all_descrptions = [tx for txs_list in all_descrptions for tx in txs_list]
    tokens = set([descr['_source']['token'] for descr in all_descrptions])
    amounts = [tx['_source']['value'] for tx in all_descrptions]
    self.assertCountEqual([2266.0, 356.24568, 356.24568, 2352.0, 100000000], amounts)
    self.assertCountEqual(['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', '0xa74476443119a942de498590fe1f2454d7d4ac0d'], tokens)
    assert len(all_descrptions) == 5

  def test_set_scanned_flags(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'total_supply': '100000000', 'blockNumber': 5000000, 'owner': '0x1554aa0026292d03cfc8a2769df8dd4d169d590a', 'parent_transaction': TEST_PARENT_TXS[0], 'cmc_id': str(1234+i), 'token_name': TEST_TOKEN_NAMES[i], 'token_symbol': TEST_TOKEN_SYMBOLS[i], 'abi': ['mock_abi'], 'decimals': 18}, id=address, refresh=True)
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_TX_INDEX, 'tx', tx, refresh=True)
    self.token_holders.get_listed_tokens_txs()
    
    tokens = self.iterate_processed()
    tokens = [t for token in tokens for t in token]
    flags = [token['_source']['tx_descr_scanned'] for token in tokens]
    self.assertCountEqual([True, True], flags)
  '''
  def test_run(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'cmc_id': str(1234+i), 'token_name': TEST_TOKEN_NAMES[i], 'token_symbol': TEST_TOKEN_SYMBOLS[i], 'abi': ['mock_abi']}, refresh=True)
    self.token_holders.run(TEST_BLOCK)
    
    all_descrptions = self.token_holders._iterate_tx_descriptions()
    all_descrptions = [tx for txs_list in all_descrptions for tx in txs_list]
    token = list(set([descr['_source']['token'] for descr in all_descrptions]))[0]
    assert token == '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d'
    assert len(all_descrptions) == 2
  '''

  def test_set_transaction_index(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[0], 'cmc_id': '1234', 'token_name': TEST_TOKEN_NAMES[0], 'token_symbol': TEST_TOKEN_SYMBOLS[0], 'abi': ['mock_abi'], 'decimals': 18}, id=TEST_TOKEN_ADDRESSES[0], refresh=True)
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_TX_INDEX, 'tx', tx, refresh=True)
    self.token_holders._extract_tokens_txs(['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d'])
    
    token_txs = self.token_holders._iterate_token_tx_descriptions('0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d')
    token_txs = [tx for txs_list in token_txs for tx in txs_list]
    tx_indices = [tx['_source']['tx_index'] for tx in token_txs]
    tx_indices = list(set(tx_indices))
    self.assertCountEqual([TEST_TX_INDEX], tx_indices)

  def test_extract_contract_creation_descr(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[0], 'total_supply': '100000000', 'blockNumber': 5000000, 'owner': '0x1554aa0026292d03cfc8a2769df8dd4d169d590a', 'parent_transaction': TEST_PARENT_TXS[0], 'cmc_id': str(1234), 'token_name': TEST_TOKEN_NAMES[0], 'token_symbol': TEST_TOKEN_SYMBOLS[0], 'abi': ['mock_abi']}, id=TEST_TOKEN_ADDRESSES[0], refresh=True)
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[1], 'total_supply': '200000000', 'blockNumber': 5000010, 'token_owner': '0x17Bc58b788808DaB201a9A90817fF3C168BF3d61', 'parent_transaction': TEST_PARENT_TXS[1], 'cmc_id': str(1235), 'token_name': TEST_TOKEN_NAMES[0], 'token_symbol': TEST_TOKEN_SYMBOLS[1], 'abi': ['mock_abi']}, id=TEST_TOKEN_ADDRESSES[1], refresh=True)
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_TX_INDEX, 'tx', tx, refresh=True)
    self.token_holders.get_listed_tokens_txs()
    
    supply_transfers = self.iterate_supply_transfers()
    supply_transfers = [t['_source'] for transfers in supply_transfers for t in transfers]
    values = [t['raw_value'] for t in supply_transfers]
    owners = [t['to'] for t in supply_transfers]
    self.assertCountEqual(['100000000', '200000000'], values)
    self.assertCountEqual(['0x1554aa0026292d03cfc8a2769df8dd4d169d590a', '0x17Bc58b788808DaB201a9A90817fF3C168BF3d61'], owners)

  def test_find_multitransfer(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[0], 'decimals': 4, 'total_supply': '100000000', 'blockNumber': 5000000, 'owner': '0x1554aa0026292d03cfc8a2769df8dd4d169d590a', 'parent_transaction': TEST_PARENT_TXS[0], 'cmc_id': str(1234), 'token_name': TEST_TOKEN_NAMES[0], 'token_symbol': TEST_TOKEN_SYMBOLS[0], 'abi': ['mock_abi']}, id=TEST_TOKEN_ADDRESSES[0], refresh=True)
    self.client.index(TEST_TX_INDEX, 'tx', TEST_MULTITRANSFER['_source'], refresh=True)
    self.token_holders.get_listed_tokens_txs()
    
    all_descrptions = self.token_holders._iterate_tx_descriptions()
    all_descrptions = [tx['_source'] for txs_list in all_descrptions for tx in txs_list]
    values = [descr['raw_value'] for descr in all_descrptions]
    assert len(all_descrptions) == 101
    assert '6000000.00000' in values

class InternalTokenTransactionsTestCase(TokenHoldersTestCase, unittest.TestCase):
  token_holders_class = InternalTokenTransactions

  def test_get_listed_tokens_itxs(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'total_supply': '100000000', 'blockNumber': 5000000, 'owner': '0x1554aa0026292d03cfc8a2769df8dd4d169d590a', 'parent_transaction': TEST_PARENT_TXS[0], 'cmc_id': str(1234+i), 'token_name': TEST_TOKEN_NAMES[i], 'token_symbol': TEST_TOKEN_SYMBOLS[i], 'abi': ['mock_abi'], 'decimals': 18}, id=address, refresh=True)
    for tx in TEST_TOKEN_ITXS:
      self.client.index(TEST_ITX_INDEX, 'itx', tx, refresh=True)
    self.token_holders.get_listed_tokens_txs()
    
    all_descrptions = self.token_holders._iterate_tx_descriptions()
    all_descrptions = [tx for txs_list in all_descrptions for tx in txs_list]
    hashes = [d['_source']['tx_hash'] for d in all_descrptions]
    self.assertCountEqual(['0x8a634bd8b381c09eec084fd7df6bdce03ccbc92f247f59d4fcc22e02131c0158', '0x04692fb0a2d1a9c8b6ea8cfc643422800b81da50df1578f3494aef0ef9be6009', '0xce37439c6809ca9d1b1d5707c7df34ceec1e4e472f0ca07c87fa449a93b02431', '0x366c6344bdb4cb1bb8cfbce5770419b03f49d631d5803e5fbcf8de9b8f1a5d66'], hashes)

  def test_set_internal_transaction_index(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[0], 'cmc_id': '1234', 'token_name': TEST_TOKEN_NAMES[0], 'token_symbol': TEST_TOKEN_SYMBOLS[0], 'abi': ['mock_abi'], 'decimals': 18}, id=TEST_TOKEN_ADDRESSES[0], refresh=True)
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_ITX_INDEX, 'itx', tx, refresh=True)
    self.token_holders._extract_tokens_txs(['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d'])
    
    token_txs = self.token_holders._iterate_token_tx_descriptions('0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d')
    token_txs = [tx for txs_list in token_txs for tx in txs_list]
    tx_indices = [tx['_source']['tx_index'] for tx in token_txs]
    tx_indices = list(set(tx_indices))
    self.assertCountEqual([TEST_ITX_INDEX], tx_indices)

TEST_INDEX = 'test-ethereum-contracts'
TEST_TX_INDEX = 'test-ethereum-txs'
TEST_ITX_INDEX = 'test-ethereum-internal-txs'
TEST_TOKEN_TX_INDEX = 'test-token-txs'

TEST_TOKEN_ADDRESSES = ['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d',
  '0xa74476443119a942de498590fe1f2454d7d4ac0d'
  ]
TEST_PARENT_TXS = ['0x8a634bd8b381c09eec084fd7df6bdce03ccbc92f247f59d4fcc22e02131c0158', '0xf349e35ce06112455d01e63ee2d447f626a88b646749c1cf2bffe474afeb703a']
TEST_TOKEN_NAMES = ['Aeternity', 'Golem Network Token']
TEST_TOKEN_SYMBOLS = ['AE', 'GNT']
TEST_TOKEN_TXS = [
  {'from': '0x6b25d0670a34c1c7b867cd9c6ad405aa1759bda0', 'to': '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', 'decoded_input': {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xa60c4c379246a7f1438bd76a92034b6c82a183a5'}, {'type': 'uint256', 'value': '2266000000000000000000'}]}, 'blockNumber': 5635149, 'hash': '0xd8f583bcb81d12dc2d3f18e0a015ef0f6e71c177913ef8f251e37b6e4f7f1f26'},
  {'from': '0x58d46475da68984bacf1f2843b85e0fdbcbc6cef', 'to': '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', 'decoded_input': {'name': 'approve', 'params': [{'type': 'address', 'value': '0x4e6b129bbb683952ed1ec935c778d74a77b352ce'}, {'type': 'uint256', 'value': '356245680000000000000'}]}, 'blockNumber': 5635141, 'hash': '0x4fc7d7027751eb605df79c63265ab83408d98179d7c8299c74a8336e5c3811ca'},
  {'from': '0xc917e19946d64aa31d1aeacb516bae2579995aa9', 'to': '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', 'error': 'Out of gas', 'decoded_input': {'name': 'transferFrom', 'params': [{'type': 'address', 'value': '0xc917e19946d64aa31d1aeacb516bae2579995aa9'}, {'type': 'address', 'value': '0x4e6b129bbb683952ed1ec935c778d74a77b352ce'}, {'type': 'uint256', 'value': '356245680000000000000'}]}, 'blockNumber': 5635142, 'hash': '0xca811570188b2e5d186da8292eda7e0bf7dde797a68d90b9ac2e014e321a94b2'},
  {'from': '0x6b25d0670a34c1c7b867cd9c6ad405aa1759bda0', 'to': '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', 'blockNumber': 5635149, 'hash': '0x2497b3dcbce36c4d2cbe42931fa160cb39703ae5487bf73044520410101e7c8c'},
  {'from': '0x892ce7dbc4a0efbbd5933820e53d2c945ef9f722', 'to': '0x51ada638582e51c931147c9abd2a6d63bc02e337', 'decoded_input': {'name': 'transfer', 'params': [{'type': 'address', 'value': '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be'}, {'type': 'uint256', 'value': '2294245680000000000000'}]}, 'blockNumber': 5632141, 'hash': '0x4188f8c914b5f58f911674ff766d45da2a19c1375a8487841dc4bdb5214c3aa2'},
  {'from': '0x930aa9a843266bdb02847168d571e7913907dd84', 'to': '0xa74476443119a942de498590fe1f2454d7d4ac0d', 'decoded_input': {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xc18118a2976a9e362a0f8d15ca10761593242a85'}, {'type': 'uint256', 'value': '2352000000000000000000'}]}, 'blockNumber': 5235141, 'hash': '0x64778c57705c4bad6b2ef8fd485052faf5c40d2197a44eb7105ce71244ded043'}
]
TEST_TOKEN_ITXS = [
  {"blockHash": "0xfdcb99de3c0bab02f7e3f38f8a74d4fd15e36dc082683763884ff6322b0c0aef", "input": "0x", "gasUsed": "0x0", "type": "call", "gas": "0x8fc", "traceAddress": [2], "transactionPosition": 42, "value": "0x13b4da79fd0e0000", "to": "0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d", "subtraces": 0, "blockNumber": 5032235, "from": "0xf04436b2edaa1b777045e1eefc6dba8bd2aebab8", "callType": "call", "output": "0x", "transactionHash": "0x366c6344bdb4cb1bb8cfbce5770419b03f49d631d5803e5fbcf8de9b8f1a5d66", 'decoded_input': {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xa60c4c379246a7f1438bd76a92034b6c82a183a5'}, {'type': 'uint256', 'value': '2266000000000000000000'}]}},
  {"blockHash": "0xfdcb99de3c0bab02f7e3f38f8a74d4fd15e36dc082683763884ff6322b0c0aef", "input": "0x", "gasUsed": "0x0", "type": "call", "gas": "0x8fc", "traceAddress": [0, 0], "transactionPosition": 89, "value": "0x1991d2e42bc5c00", "to": "0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d", "subtraces": 0, "blockNumber": 5032235, "from": "0xa36ae0f959046a18d109dc5b1fb8df655cf0aa81", "callType": "call", "output": "0x", "transactionHash": "0xce37439c6809ca9d1b1d5707c7df34ceec1e4e472f0ca07c87fa449a93b02431", 'decoded_input': {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xa60c4c379246a7f1438bd76a92034b6c82a183a5'}, {'type': 'uint256', 'value': '2266000000000000000000'}]}},
  {"blockHash": "0xfdcb99de3c0bab02f7e3f38f8a74d4fd15e36dc082683763884ff6322b0c0aef", "input": "0xc281d19e", "gasUsed": "0x5a4", "type": "call", "gas": "0x303d8", "traceAddress": [1], "transactionPosition": 102, "value": "0x0", "to": "0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d", "subtraces": 0, "blockNumber": 5032235, "from": "0xd91e45416bfbbec6e2d1ae4ac83b788a21acf583", "callType": "call", "output": "0x00000000000000000000000026588a9301b0428d95e6fc3a5024fce8bec12d51", "transactionHash": "0x04692fb0a2d1a9c8b6ea8cfc643422800b81da50df1578f3494aef0ef9be6009", 'decoded_input': {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xa60c4c379246a7f1438bd76a92034b6c82a183a5'}, {'type': 'uint256', 'value': '2266000000000000000000'}]}}
]
TEST_MULTITRANSFER = {'_source': {
  "blockTimestamp" : "2018-01-06T01:51:34",
  "from" : "0x8c77a38c5dfec301a0d67904ce1850812c42b0e7",
  "creates" : None,
  "transactionIndex" : 54,
  "hash" : "0xb7cafeb740f7342e0c6877ac1e9fb86d0c69b8060d497d16d9d5b457504f48f1",
  "blockNumber" : 4860873,
  "value" : 0.0,
  "to" : "0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d",
  "decoded_input" : {
    "name" : "multiTransfer",
    "params" : [
      {
        "type" : "address[]",
        "value" : "[\"0x02a760e8d376847ac803510eabe114314abdfd89\", \"0xfdaba975cbdf83d8044563b5a8f7f3c5469181b2\", \"0x002f9caf40a444f20813da783d152bdfaf42852f\", \"0x42e7aea5fbeac52d3061dd0d1d884912dd891279\", \"0x208dd8741d6e9801853d7bcd2eecfce203482de8\", \"0x1006ecd6ec35d85a9d43cc12898369bfcee7f7ca\", \"0x23acd97dae368b2b3e4aa739488518d2d2fff9e3\", \"0xef91baf35935d753b967c61a5b9fe961aa5f629e\", \"0x139d2b44e27114663c57990b25b662cb59641af2\", \"0x0e57a994a01b025651b0e2c6d18cf482ea72b445\", \"0x02f951099c11f047e8ccf699e8320dfd7871a5c5\", \"0xb3627223e8f514e8af85fce8b715d80fb66ff634\", \"0x1cd6982c89b259663ff02929669835f4ba7dfff4\", \"0x00d1411e66aa7d0ba9d4d17269a42bfbb98d4019\", \"0x49c7cf2c2732b3c93372f2e1e871fa81888ac959\", \"0x9d8c8d4ccb3748fb957095f9c3ededeff51c5707\", \"0xfa78064ab9612609840adf0d25d43eabcc11c92d\", \"0x074367ff88b650827091db6c6ac334a080a1f591\", \"0xe4fae0753f053e2967ea4da87ba12f1e9dcfe9a9\", \"0xc045dc4265b7223bf7bcc2de90fa3586573bc57f\", \"0x44acb7665310fe891686f68dd19df0225a274a41\", \"0x44a0e10b62a1a06dee52f8a404f7412bb72f05f5\", \"0xa3fa9d47684586773df41c2f611093933aa85771\", \"0x570208193f67c138ba3dd60a3aee9b0fa9d574c2\", \"0x6639ca6260c7d7d5387412884146290f65f97432\", \"0x493d7ac81c2fb143c44bf19a01bc1d038a8a49b4\", \"0x3139b4870b187411e8d164b9faa460619d3df167\", \"0x9a80b885c93250ca528a3e2f519738dbc46ba539\", \"0x64b8033738740946cf455d569555ed2c24f6144c\", \"0xc9a7cfbd732075d53577e732b770cdec45eb84c3\", \"0xefe671929ea3a4c555c48ffe695b14ef619490e0\", \"0x7864144492697c155671a50d5808c0f42204a2ec\", \"0xd3f59bba237571ca7b5644e9793e107cdaf5c175\", \"0x587486675d9f16f6e98b1c5fe28a222cf40917e6\", \"0xe4c50cc7bfaccb23ac4e4e146990dec93a70c065\", \"0x24b9da86e6c539c32bf27001750a01592d42f83a\", \"0xc0bf9f8f671d45eff6578c06fe0cef2bfd63ab70\", \"0xd231beea4853666f91d6495c734af898171ed7a5\", \"0xccde2e9ffc9e65508d3fd1d0ca50fce553ef8a5b\", \"0xbb0cf974e9d15eacab1967e2c845faf0143e57cd\", \"0xd88be48a86b3621a3422264f9003946d6b833c29\", \"0x14460928c7c4ab00a624e466ef8b5ba4f59de497\", \"0xf2dcebb1cb140efea42c9dc0ae0d6fd928d79ff2\", \"0x16dbb766ce89aa21a4fc7517a06318b74a149d5a\", \"0x430c9d4d7bdaaa567df9a660e811706ff6b1ba3d\", \"0x241a257fdd0570f0ab24a5d74525620396e6830d\", \"0x8ba081932029474ddb567eb84ecbee0dcd4bfd65\", \"0xfd16911300f1f6c73f1403b6176b2ad269351eca\", \"0xe6e94534b21afd00e9fa3f7e057cb6ce5534a0e5\", \"0x66e5ac60e5d69d63d95c1551ca935ed0ea1ae2d7\", \"0xdfe5fe83e42364b702fdce581e71e52f9f8cf406\", \"0x3274d1179cee7dd5eb35833a35f27d157ec8625f\", \"0xbf9e106e149749428b0a832a9f9ac2189c8d6add\", \"0x57bca658909d3445e8c6c8aee83a8f0ff94cc962\", \"0x0fccbdeb3ff7af9005d7d34ec1f73715e8d62029\", \"0x4f49deee07eb3bfd702668c74f827f284db6c92b\", \"0x8493d85bd0b7bbbb2b54f20d0e7cd924259b511e\", \"0xe95fc92f3c4f730dd1f209dc1f5f85313e2c1932\", \"0xae072dfd667315f8bc36e29bdd485be3b11423b3\", \"0xffa9476cc03a0749cb73a625c30e82342d2c9398\", \"0x62d7c830e06b89bb50cc0cf28dcbbe9da45743de\", \"0xecb387abb0bac1e4156aed603f1829e20244dd68\", \"0x3cd6a6dca7dbde5da59f0ba85968ce833fdfb61b\", \"0x6b99e5d75e3f7050e7a19cee4804f32f57ce31cb\", \"0x18176762376cb0bddadcc798df25ff0b62f730ee\", \"0xc6c764fc6c1e1211d2b4a06ef2170f660a4512fa\", \"0x9a16558e7e6d3f14c00d866910a3cd9a6b76b432\", \"0xd982ba6c0c98e956169d10edec136a401b0df7ce\", \"0x26c11318a039ebc95a9a502f7d251e15e154994b\", \"0x2227969b5296493665a68b8d2921887a7d376291\", \"0xf5972b1ed3d95f54ee0c4ce5c131f8598c4804af\", \"0xef16afacf9f7198a486bf8a7af5310c509e464b9\", \"0x379cc610580a4f3686aaf98af102973e092418fb\", \"0x7079311ba13a1ee7c865e63a13b83724a3efa09b\", \"0x611a6cab8480e89ffb4680eadb7a1cb097aa71ed\", \"0x55a7c0ba5c136b5bc71f6e630793d5c831c15ae9\", \"0x49a38a586dce1d8cd4008e46ef104c21282174a9\", \"0x542dd143c687c72973a19e96214b651e055d7515\", \"0x5caf1971339299811b5cb53c673b97ebcbb7fc78\", \"0xf62ae37accd4c1bf6d0310ff82b2a03acc4bda02\", \"0xd70b3fd24658481f3f938f822aabb14d78064c84\", \"0x3ee5a8462b3a1ffeb2d29644f903f8e97a31961f\", \"0xd40201fc64f2980ac74bfe0c6450d7ac552bc714\", \"0x460cf14a8a6006cd359c834102f7bc9211317c87\", \"0xac2a426edeac9a2352c0daf0a87b5ecf2930a940\", \"0x103640c80ef054cb3184a15fa9461876bb905343\", \"0x366f77a6d702148e3e2f166e9806f89b5f79c489\", \"0x50b30fd5645ee94b62cc2c1ac14b76360f4b3341\", \"0x0bc7411b8b3eb678b54c76403b41ad37fb29ab0e\", \"0x7d0bcf7165989cbd8bf43b62eadfce4628cd3b6b\", \"0x9c519ccaaac0e9abc993c41f44e03255e743693a\", \"0xee94f87415834df846ae1eaa445d6a89b9f4a671\", \"0xaa9cd922aaa946793e232fe1974b991d900e50c5\", \"0x7616b84a0178758c7581e89f932feda045213f26\", \"0xf607da0c6a8641dcfa7a5db01ea9ac060dedaf6b\", \"0xfbbec8dcf2174ff100ea13062f3064edbe5ffeee\", \"0xcb7c2aecbb5dddaabbfcd3f81179bf2f6cffcd0e\", \"0x8bdb923d59f434f578352925659c1f07ee5d00ea\", \"0xd367cc3359717b7b5a4697cd1929c18cf42d8a0d\", \"0xe5d5a257b1e877d1388f022d3cd8ed0b83a553ca\"]"
      },
      {
        "type" : "uint256[]",
        "value" : "[31873669002669, 3750000000000, 3160000000000, 2171376924597, 2000000000000, 1800040000000, 1100000000000, 1100000000000, 1100000000000, 1050000000000, 946147600200, 550000000000, 515000000000, 471750000000, 440000000000, 399942250572, 375000000000, 352000000000, 339851490000, 337162708912, 320250000000, 299984249250, 299625000000, 249480000000, 249028889952, 225000000000, 225000000000, 220000000000, 220000000000, 219749929806, 219686162625, 218533014780, 215000002500, 210000000000, 210000000000, 200000000000, 200000000000, 191386565041, 183750000000, 183648047323, 177925380000, 165830000000, 165000000000, 161896127444, 158045977011, 153702750000, 150955900000, 150000000000, 150000000000, 150000000000, 143159848393, 114400000000, 112115073123, 110000000000, 110000000000, 110000000000, 110000000000, 108900000000, 108459127920, 105000000000, 103950000000, 103500000000, 102711681000, 102142871735, 100837728570, 100000000000, 100000000000, 89625000000, 84407364298, 84000000000, 81118464733, 79807347056, 75500236709, 75198356250, 75000000000, 75000000000, 75000000000, 74935425000, 74850000000, 74750350308, 74037556192, 73706134869, 73500000000, 73306028250, 69796891047, 64851490000, 63346799204, 60000000000, 55000000000, 55000000000, 53169736637, 50600000000, 46014817883, 45000000000, 44000000000, 43698339480, 42130000000, 41718055500, 37500000000, 37500000000]"
      }
    ]
  }
}}
TEST_BLOCK = {
  "difficulty": "0xb49b6e4f02608",
  "extraData": "0x737061726b706f6f6c2d636e2d6e6f64652d39",
  "gasLimit": "0x79f39e",
  "gasUsed": "0x79dd4c",
  "hash": "0xfe5ea4c58e05a534570f9bd685a9e6e7e0e505f971757e185417845efc2434c2",
  "logsBloom": "0x000219850100a0e020208420010004434012900122000000800048410300420091d100000001004041811060231010000040a000301000040483010110282810008410010c10801c00000009040022a400000014003904012400000880241524000006520a014000100a000000400a200a8008420c104000a11600b0100094101040470442a000080084400a10200090006249001050000920001428800000808a040000200400004c00000000400000000000580a05000904400200800901149822040384704120400280008000100403e50040300e0c0204000241402822c00014036004000182001220880040101540c00026104000004a001200d0000082",
  "miner": "0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c",
  "mixHash": "0xec267c36380cafe9ee9b6d87a8435d2b836c4143f1ad5039d7bd42bdf3d6f509",
  "nonce": "0x405fb3c402b65764",
  "number": "0x567e6d",
  "parentHash": "0x0614307e4c441119386ac91a1caca04687607fd352f350acee404157bc9757aa",
  "receiptsRoot": "0xe11b2005cdf839bc64bcc4ff310fc4a1ba49ff45356ddd131b797ae87483a409",
  "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
  "size": "0x719d",
  "stateRoot": "0xfaf53ba964b5288bcf961a6a48be58f9d7e40ee6c317446d8ab45a94b38ebfb0",
  "timestamp": "0x5b06a409",
  "totalDifficulty": "0xed153ab1b63888923c",
  "transactions": [
    {
      "blockHash": "0xfe5ea4c58e05a534570f9bd685a9e6e7e0e505f971757e185417845efc2434c2",
      "blockNumber": "0x567e6d",
      "from": "0x2ab5aa1d5212d2c92c6a8734222c1eee11ed8547",
      "gas": "0x8fd2",
      "gasPrice": "0x98bca5a00",
      "hash": "0x42619022e399e6dd573dec33cff97f4cb0618922620c5db1c5991448868a014d",
      "input": "0xa9059cbb0000000000000000000000000631787216194b4f13e34db7702793c331c5e66900000000000000000000000000000000000000000000000000000000c6b30120",
      "nonce": "0x1f",
      "to": "0xc50948bac01116f246259070ea6084c04649efdf",
      "transactionIndex": "0x0",
      "value": "0x0",
      "v": "0x25",
      "r": "0xfa1f8d5d5831a6f81225cdd0ee4c98e0288556ec11c737dc73c2aedd04b96131",
      "s": "0x7ff03151e788bdeabbc6ec02c78b72cbb4b6b3c0e2fa8a3819bf56d0717c87b9",
      "decoded_input": {
        'name': 'transfer', 
        'params': [
          {
            'type': 'address', 
            'value': '0xa60c4c379246a7f1438bd76a92034b6c82a183a5'
          }, 
          {
            'type': 'uint256', 
            'value': '2266000000000000000000'
          }
        ]
      }
    },
    {
      "blockHash": "0xfe5ea4c58e05a534570f9bd685a9e6e7e0e505f971757e185417845efc2434c2",
      "blockNumber": "0x567e6d",
      "from": "0x2711301985fd072c3c94b45781d4897f44c562d5",
      "gas": "0x15f90",
      "gasPrice": "0x826299e00",
      "hash": "0xfa8b523e944961883e2cdfee3413dab7c299f861d7ddd6fa11004a0e7a3ea133",
      "input": "0xa9059cbb0000000000000000000000007b1becff5ffe89f9c950f92f07c65bb29613f06700000000000000000000000000000000000000000000000f732b66015a53ffff",
      "nonce": "0x4290",
      "to": "0x7537aef853f63f114e6152956faf26488c08cc84",
      "transactionIndex": "0x1",
      "value": "0x0",
      "v": "0x26",
      "r": "0xe41c53795fe010946a968ddbf5724b201250efbd416b0880d907025de4048ca5",
      "s": "0xad756bb5e4592a2156fbf661dc15667b32a4a9ad26062e9795fd02fc5b1fb55",
      "decoded_input": {
        'name': 'transferFrom', 
        'params': [
          {
            'type': 'address', 
            'value': '0xc917e19946d64aa31d1aeacb516bae2579995aa9'
          }, 
          {
            'type': 'address', 
            'value': '0x4e6b129bbb683952ed1ec935c778d74a77b352ce'
          }, 
          {
            'type': 'uint256', 
            'value': '356245680000000000000'
          }
        ]
      }
    },
    {
      "blockHash": "0xfe5ea4c58e05a534570f9bd685a9e6e7e0e505f971757e185417845efc2434c2",
      "blockNumber": "0x567e6d",
      "from": "0x14356ba2091d320b5e7e7880e78c4f5673acc5e3",
      "gas": "0x1d4c0",
      "gasPrice": "0x312c80400",
      "hash": "0x288851a90e18d4c67bc2e5adfb58e5443b9126ab0f6f08dc5c5f8e1eb4ebd37d",
      "input": "0xa9059cbb000000000000000000000000fe5854255eb1eb921525fa856a3947ed2412a1d700000000000000000000000000000000000000000000098774738bc822200000",
      "nonce": "0x5",
      "to": "0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d",
      "transactionIndex": "0x2c",
      "value": "0x0",
      "v": "0x26",
      "r": "0x424a69f113ab7efebcc3838e6313b6512ea14013cb6a7622e4a61fc180770fe6",
      "s": "0x4e85edf81800f908dfdadbab82362cbc48213f15853a855131ef82bdd92f782a",
      "decoded_input": {
        'name': 'transferFrom', 
        'params': [
          {
            'type': 'address', 
            'value': '0xc917e19946d64aa31d1aeacb516bae2579995aa9'
          }, 
          {
            'type': 'address', 
            'value': '0x4e6b129bbb683952ed1ec935c778d74a77b352ce'
          }, 
          {
            'type': 'uint256', 
            'value': '356245680000000000000'
          }
        ]
      }
    },
    {
      "blockHash": "0xfe5ea4c58e05a534570f9bd685a9e6e7e0e505f971757e185417845efc2434c2",
      "blockNumber": "0x567e6d",
      "from": "0x4416537e37cd59c217f97c6a816a6a4eee21051a",
      "gas": "0x1d4c0",
      "gasPrice": "0x312c80400",
      "hash": "0x9142381d6f304010d2442593da79eab297b224a6554cc2ebf9b4c2056c9d00e6",
      "input": "0xa9059cbb000000000000000000000000fe5854255eb1eb921525fa856a3947ed2412a1d700000000000000000000000000000000000000000000001d52ce324cf5124000",
      "nonce": "0x16",
      "to": "0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d",
      "transactionIndex": "0x2d",
      "value": "0x0",
      "v": "0x26",
      "r": "0xc5af0af7fa306fb718fa684c3d77c396ff6d08aea2468e6e880a54b6fbb5476e",
      "s": "0x6e0e7f06768bb7be5691aafc96a963f25dadd4e1237b5091486e3d60bcac3fe3",
      "decoded_input": {
        'name': 'transferFrom', 
        'params': [
          {
            'type': 'address', 
            'value': '0xc917e19946d64aa31d1aeacb516bae2579995aa9'
          }, 
          {
            'type': 'address', 
            'value': '0x4e6b129bbb683952ed1ec935c778d74a77b352ce'
          }, 
          {
            'type': 'uint256', 
            'value': '356245680000000000000'
          }
        ]
      }
    },
    {
      "blockHash": "0xfe5ea4c58e05a534570f9bd685a9e6e7e0e505f971757e185417845efc2434c2",
      "blockNumber": "0x567e6d",
      "from": "0x61e463617ef75e528f90f92fcefb927ef735ae42",
      "gas": "0xea60",
      "gasPrice": "0x2aa19c470",
      "hash": "0xd68c3337e37f5bdd4b5d15378e660c9f18ea81a28c742f1a16ea8f49d784f9fc",
      "input": "0xa9059cbb000000000000000000000000698e25fc99b355fce28160d6c641e7c9c777ffce0000000000000000000000000000000000000000000000000000010c6531d540",
      "nonce": "0x1",
      "to": "0x4ccc3759eb48faf1c6cfadad2619e7038db6b212",
      "transactionIndex": "0x40",
      "value": "0x0",
      "v": "0x26",
      "r": "0xa231507e58befaf61112ecd768d50ae4b4ae616beb26720767b5687484171df4",
      "s": "0x52f169ec07ef4b1f4632e15cb2a9c36d8ddaab8510c7d417105e21808ba63941",
      "decoded_input": {
        'name': 'transferFrom', 
        'params': [
          {
            'type': 'address', 
            'value': '0xc917e19946d64aa31d1aeacb516bae2579995aa9'
          }, 
          {
            'type': 'address', 
            'value': '0x4e6b129bbb683952ed1ec935c778d74a77b352ce'
          }, 
          {
            'type': 'uint256', 
            'value': '356245680000000000000'
          }
        ]
      }
    }
  ],
  "transactionsRoot": "0x5a2326af8886123c47abcccd5aa2b05c8842de2b72674ae040decf108f496fa8",
  "uncles": []
}
    