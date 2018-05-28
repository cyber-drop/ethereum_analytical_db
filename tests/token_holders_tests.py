import unittest
from token_holders import TokenHolders
from test_utils import TestElasticSearch

class TokenHoldersTestCase(unittest.TestCase):
  def setUp(self):
    self.client = TestElasticSearch()
    self.client.recreate_index(TEST_INDEX)
    self.client.recreate_index(TEST_TX_INDEX)
    self.client.recreate_index(TEST_LISTED_INDEX)
    self.client.recreate_index(TEST_TOKEN_TX_INDEX)
    self.token_holders = TokenHolders({'contract': TEST_INDEX, 'transaction': TEST_TX_INDEX, 'listed_token': TEST_LISTED_INDEX, 'token_tx': TEST_TOKEN_TX_INDEX})
  
  def test_get_listed_tokens(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'token_name': TEST_TOKEN_NAMES[i]}, refresh=True)

    listed_tokens = self.token_holders._get_listed_tokens()
    listed_tokens = [token[0]['_source']['token_name'] for token in listed_tokens]
    listed_tokens = set(listed_tokens)
    self.assertCountEqual(['Aeternity', 'Populous Platform', 'Golem Network Token'], listed_tokens)
  
  def test_search_duplicates(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'token_name': TEST_TOKEN_NAMES[i]}, refresh=True)
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_TX_INDEX, 'tx', tx, refresh=True)

    listed_tokens = self.token_holders._search_duplicates()
    duplicated = [token for token in listed_tokens if token['duplicated'] == True]
    print(duplicated)
    real_golem = [token for token in duplicated if token['token_name'] == 'Golem Network Token'][0]
    real_aeternity = [token for token in duplicated if token['token_name'] == 'Aeternity'][0]
    assert real_golem['address'] == '0xa74476443119a942de498590fe1f2454d7d4ac0d'
    assert real_aeternity['address'] == '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d'
    assert real_aeternity['txs_count'] == 3
  
  def test_extract_token_txs(self):
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_TX_INDEX, 'tx', tx, refresh=True)

    self.token_holders._extract_token_txs('0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d')
    token_txs = self.token_holders._iterate_token_tx_descriptions('0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d')
    token_txs = [tx for txs_list in token_txs for tx in txs_list]
    methods = [tx['_source']['method'] for tx in token_txs]
    amounts = [tx['_source']['value'] for tx in token_txs] 
    self.assertCountEqual(['transfer', 'approve', 'transferFrom'], methods)
    self.assertCountEqual(['356245680000000000000', '356245680000000000000', '2266000000000000000000'], amounts)
  
  def test_get_listed_tokens_txs(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'token_name': TEST_TOKEN_NAMES[i]}, refresh=True)
    for tx in TEST_TOKEN_TXS:
      self.client.index(TEST_TX_INDEX, 'tx', tx, refresh=True)

    self.token_holders.get_listed_tokens_txs()
    all_descrptions = self.token_holders._iterate_tx_descriptions()
    all_descrptions = [tx for txs_list in all_descrptions for tx in txs_list]
    tokens = set([descr['_source']['token'] for descr in all_descrptions])
    self.assertCountEqual(['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', '0xa74476443119a942de498590fe1f2454d7d4ac0d'], tokens)
    assert len(all_descrptions) == 4
  
  def test_run(self):
    for i, address in enumerate(TEST_TOKEN_ADDRESSES):
      self.client.index(TEST_INDEX, 'contract', {'address': address, 'token_name': TEST_TOKEN_NAMES[i]}, refresh=True)

    self.token_holders._load_listed_tokens()
    self.token_holders.run(TEST_BLOCK)

    all_descrptions = self.token_holders._iterate_tx_descriptions()
    all_descrptions = [tx for txs_list in all_descrptions for tx in txs_list]
    token = list(set([descr['_source']['token'] for descr in all_descrptions]))[0]
    assert token == '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d'
    assert len(all_descrptions) == 2
  
TEST_INDEX = 'test-ethereum-contracts'
TEST_TX_INDEX = 'test-ethereum-txs'
TEST_LISTED_INDEX = 'test-listed-tokens'
TEST_TOKEN_TX_INDEX = 'test-token-txs'

TEST_TOKEN_ADDRESSES = ['0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d',
  '0xd4fa1460f537bb9085d22c7bccb5dd450ef28e3a',
  '0x51ada638582e51c931147c9abd2a6d63bc02e337',
  '0xa74476443119a942de498590fe1f2454d7d4ac0d',
  '0xbe78d802c2aeebdc34c810b805c2691885a61257',
  '0x83199a2bd905dd5f2f61828e5a705790b782cf43'
  ]
TEST_TOKEN_NAMES = ['Aeternity', 'Populous Platform', 'Aeternity', 'Golem Network Token', 'Golem Network Token', 'Samtoken']

TEST_TOKEN_TXS = [
  {'from': '0x6b25d0670a34c1c7b867cd9c6ad405aa1759bda0', 'to': '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', 'decoded_input': {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xa60c4c379246a7f1438bd76a92034b6c82a183a5'}, {'type': 'uint256', 'value': '2266000000000000000000'}]}, 'blockNumber': 5635149},
  {'from': '0x58d46475da68984bacf1f2843b85e0fdbcbc6cef', 'to': '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', 'decoded_input': {'name': 'approve', 'params': [{'type': 'address', 'value': '0x4e6b129bbb683952ed1ec935c778d74a77b352ce'}, {'type': 'uint256', 'value': '356245680000000000000'}]}, 'blockNumber': 5635141},
  {'from': '0xc917e19946d64aa31d1aeacb516bae2579995aa9', 'to': '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', 'decoded_input': {'name': 'transferFrom', 'params': [{'type': 'address', 'value': '0xc917e19946d64aa31d1aeacb516bae2579995aa9'}, {'type': 'address', 'value': '0x4e6b129bbb683952ed1ec935c778d74a77b352ce'}, {'type': 'uint256', 'value': '356245680000000000000'}]}, 'blockNumber': 5635142},
  {'from': '0x6b25d0670a34c1c7b867cd9c6ad405aa1759bda0', 'to': '0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d', 'hash': '0xfa8b523e944961883e2cdfee3413dab7c299f861d7ddd6fa11004a0e7a3ea133', 'blockNumber': 5635149},
  {'from': '0x892ce7dbc4a0efbbd5933820e53d2c945ef9f722', 'to': '0x51ada638582e51c931147c9abd2a6d63bc02e337', 'decoded_input': {'name': 'transfer', 'params': [{'type': 'address', 'value': '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be'}, {'type': 'uint256', 'value': '2294245680000000000000'}]}, 'blockNumber': 5632141},
  {'from': '0x930aa9a843266bdb02847168d571e7913907dd84', 'to': '0xa74476443119a942de498590fe1f2454d7d4ac0d', 'decoded_input': {'name': 'transfer', 'params': [{'type': 'address', 'value': '0xc18118a2976a9e362a0f8d15ca10761593242a85'}, {'type': 'uint256', 'value': '2352000000000000000000'}]}, 'blockNumber': 5235141}
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
    