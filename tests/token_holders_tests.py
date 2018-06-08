import unittest
from token_holders import TokenHolders, ExternalTokenTransactions, InternalTokenTransactions
from test_utils import TestElasticSearch

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

  def test_extract_token_txs(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[0], 'cmc_listed': True, 'token_name': TEST_TOKEN_NAMES[0], 'token_symbol': TEST_TOKEN_SYMBOLS[0], 'abi': ['mock_abi'], 'decimals': 18}, id=TEST_TOKEN_ADDRESSES[0], refresh=True)
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

  def test_get_listed_tokens_txs(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'cmc_listed': True, 'token_name': TEST_TOKEN_NAMES[i], 'token_symbol': TEST_TOKEN_SYMBOLS[i], 'abi': ['mock_abi'], 'decimals': 18}, id=address, refresh=True)
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_TX_INDEX, 'tx', tx, refresh=True)
    self.token_holders.get_listed_tokens_txs()
    all_descrptions = self.token_holders._iterate_tx_descriptions()
    all_descrptions = [tx for txs_list in all_descrptions for tx in txs_list]
    tokens = set([descr['_source']['token'] for descr in all_descrptions])
    amounts = [tx['_source']['value'] for tx in all_descrptions]
    self.assertCountEqual([2266.0, 356.24568, 356.24568, 2352.0], amounts)
    self.assertCountEqual(['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', '0xa74476443119a942de498590fe1f2454d7d4ac0d'], tokens)
    assert len(all_descrptions) == 4

  def test_set_scanned_flags(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'cmc_listed': True, 'token_name': TEST_TOKEN_NAMES[i], 'token_symbol': TEST_TOKEN_SYMBOLS[i], 'abi': ['mock_abi'], 'decimals': 18}, id=address, refresh=True)
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_TX_INDEX, 'tx', tx, refresh=True)
    self.token_holders.get_listed_tokens_txs()
    tokens = self.token_holders._iterate_tokens()
    tokens = [t for token in tokens for t in token]
    flags = [token['_source']['tx_descr_scanned'] for token in tokens]
    self.assertCountEqual([True, True], flags)

  def test_run(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'cmc_listed': True, 'token_name': TEST_TOKEN_NAMES[i], 'token_symbol': TEST_TOKEN_SYMBOLS[i], 'abi': ['mock_abi']}, refresh=True)
    self.token_holders.run(TEST_BLOCK)
    all_descrptions = self.token_holders._iterate_tx_descriptions()
    all_descrptions = [tx for txs_list in all_descrptions for tx in txs_list]
    token = list(set([descr['_source']['token'] for descr in all_descrptions]))[0]
    assert token == '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d'
    assert len(all_descrptions) == 2

  def test_set_transaction_index(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[0], 'cmc_listed': True, 'token_name': TEST_TOKEN_NAMES[0], 'token_symbol': TEST_TOKEN_SYMBOLS[0], 'abi': ['mock_abi'], 'decimals': 18}, id=TEST_TOKEN_ADDRESSES[0], refresh=True)
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_TX_INDEX, 'tx', tx, refresh=True)
    self.token_holders._extract_tokens_txs(['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d'])
    token_txs = self.token_holders._iterate_token_tx_descriptions('0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d')
    token_txs = [tx for txs_list in token_txs for tx in txs_list]
    tx_indices = [tx['_source']['tx_index'] for tx in token_txs]
    tx_indices = list(set(tx_indices))
    self.assertCountEqual([TEST_TX_INDEX], tx_indices)

class InternalTokenTransactionsTestCase(TokenHoldersTestCase, unittest.TestCase):
  token_holders_class = InternalTokenTransactions

  def test_get_listed_tokens_itxs(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'cmc_listed': True, 'token_name': TEST_TOKEN_NAMES[i], 'token_symbol': TEST_TOKEN_SYMBOLS[i], 'abi': ['mock_abi'], 'decimals': 18}, id=address, refresh=True)
    for tx in TEST_TOKEN_ITXS:
      self.client.index(TEST_ITX_INDEX, 'itx', tx, refresh=True)
    self.token_holders.get_listed_tokens_txs()
    all_descrptions = self.token_holders._iterate_tx_descriptions()
    all_descrptions = [tx for txs_list in all_descrptions for tx in txs_list]
    hashes = [d['_source']['tx_hash'] for d in all_descrptions]
    self.assertCountEqual(['0x04692fb0a2d1a9c8b6ea8cfc643422800b81da50df1578f3494aef0ef9be6009', '0xce37439c6809ca9d1b1d5707c7df34ceec1e4e472f0ca07c87fa449a93b02431', '0x366c6344bdb4cb1bb8cfbce5770419b03f49d631d5803e5fbcf8de9b8f1a5d66'], hashes)

  def test_set_internal_transaction_index(self):
    self.client.index(TEST_INDEX, 'contract', {'address': TEST_TOKEN_ADDRESSES[0], 'cmc_listed': True, 'token_name': TEST_TOKEN_NAMES[0], 'token_symbol': TEST_TOKEN_SYMBOLS[0], 'abi': ['mock_abi'], 'decimals': 18}, id=TEST_TOKEN_ADDRESSES[0], refresh=True)
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
    