import unittest
from tests.test_utils import TestElasticSearch
from operations.indices import ElasticSearchIndices, ClickhouseIndices
from unittest.mock import MagicMock
import subprocess
from time import time
from tests.test_utils import mockify
from tests.test_utils import TestClickhouse
from datetime import datetime

class ElasticSearchIndicesTestCase(unittest.TestCase):
  string_fields = ["callType", "from", "gas", "hash", "blockTimestamp", "gasUsed",
                   "blockHash", "transactionHash", "refundAddress", "to",
                   "type", "address", "balance", "blockNumber"]
  text_fields = ["input", "code", "init", "error", "output"]
  object_fields = ["traceAddress", "decoded_input"]
  doc_type = 'itx'
  doc = {
    "input": "0x000000000000000000000000000000000000000000000000000000000032a44c",
    "type": "call",
    "output": "0x0000000000000000000000000000000000000000000000000000000000000001",
    "subtraces": 1,
    "gasUsed": "0x6",
    "from": "0x6a0a0fc761c612c340a0e98d33b37a75e5268472",
    "gas": "0x6",
    "value": 0,
    "class": 3,
    "to": "0x0f045b8a7f4587cdff0919fe8d12613a7e1b7230",
    "callType": "call",
    "blockHash": "0x31864a7a7ed528fe40156126c52fdcc8cdefaf692a92e8b394e108df91dbe106",
    "transactionHash": "0xd05ab992923b075bbf4b6be784cc4b386d3acaf41af9760d0465eff8499d3b3e",
    "refundAddress": "0x12ef7e5ff5693849fcbb7e06e6376686b4499ffd",
    "code": "0x606060405263ffffffff60e060020a6000350416636ea056a98114610021575bfe5b341561002957fe5b610040600160a060020a0360043516602435610054565b604080519115158252519081900360200190f35b6000805460408051602090810184905281517f3c18d318000000000000000000000000000000000000000000000000000000008152600160a060020a03878116600483015292519290931692633c18d318926024808301939282900301818787803b15156100be57fe5b60325a03f115156100cb57fe5b50505060405180519050600160a060020a0316600036600060405160200152604051808383808284378201915050925050506020604051808303818560325a03f4151561011457fe5b50506040515190505b929150505600a165627a7a7230582072faa239cc9c48e881b02f074d012a710ff574cc3be6ae9a976f28ad2aaaf6710029",
    "init": "0x6060604052341561000c57fe5b60405160208061019a83398101604052515b60008054600160a060020a031916600160a060020a0383161790555b505b61014f8061004b6000396000f300606060405263ffffffff60e060020a6000350416636ea056a98114610021575bfe5b341561002957fe5b610040600160a060020a0360043516602435610054565b604080519115158252519081900360200190f35b6000805460408051602090810184905281517f3c18d318000000000000000000000000000000000000000000000000000000008152600160a060020a03878116600483015292519290931692633c18d318926024808301939282900301818787803b15156100be57fe5b60325a03f115156100cb57fe5b50505060405180519050600160a060020a0316600036600060405160200152604051808383808284378201915050925050506020604051808303818560325a03f4151561011457fe5b50506040515190505b929150505600a165627a7a7230582072faa239cc9c48e881b02f074d012a710ff574cc3be6ae9a976f28ad2aaaf67100290000000000000000000000004f01001cf69785d4c37f03fd87398849411ccbba",
    "error": "Out of gas",
    "traceAddress": [1, 0],
    "decoded_input": {
      "name": "approve",
      "params": [
        {
          "type": "address",
          "value": "0x8d12a197cb00d4747a1fe03395095ce2a5cc6819"
        },
        {
          "type": "uint256",
          "value": "50000000000000000000000000"
        }
      ]
    }
  }
  index_methods = [
    '_create_index_with_best_compression',
    '_set_max_result_size'
  ]
  mapping_methods = [
    '_set_object_properties_mapping',
    '_set_string_properties_mapping',
    '_set_text_properties_mapping',
    '_disable_all_field'
  ]

  def setUp(self):
    self.client = TestElasticSearch()
    self.prepare_indices = ElasticSearchIndices(TEST_INDICES)
    self.client.recreate_index(TEST_INDEX)

  def test_string_properties_mapping(self):
    self.prepare_indices._set_string_properties_mapping(index=TEST_INDEX, doc_type=self.doc_type)
    mapping = self.client.get_mapping(index=TEST_INDEX)
    mapping_fields = mapping[TEST_INDEX]['mappings'][self.doc_type]['properties']
    for field in self.string_fields:
      mapping_field = mapping_fields[field]
      assert mapping_field["type"] == "keyword"

  def test_text_properties_mapping(self):
    self.prepare_indices._set_text_properties_mapping(index=TEST_INDEX, doc_type=self.doc_type)
    mapping = self.client.get_mapping(index=TEST_INDEX)
    mapping_fields = mapping[TEST_INDEX]['mappings'][self.doc_type]['properties']
    for field in self.text_fields:
      mapping_field = mapping_fields[field]
      assert mapping_field["type"] == "text"
      assert not mapping_field["index"]
      assert mapping_field['fields']["keyword"]["ignore_above"] > 100

  def test_object_properties_mapping(self):
    self.prepare_indices._set_object_properties_mapping(index=TEST_INDEX, doc_type=self.doc_type)
    mapping = self.client.get_mapping(index=TEST_INDEX)
    mapping_fields = mapping[TEST_INDEX]['mappings'][self.doc_type]['properties']
    for field in self.object_fields:
      mapping_field = mapping_fields[field]
      assert not mapping_field["enabled"]

  def test_disable_all_field(self):
    self.prepare_indices._disable_all_field(index=TEST_INDEX, doc_type=self.doc_type)
    mapping = self.client.get_mapping(index=TEST_INDEX)
    all_field_mapping = mapping[TEST_INDEX]['mappings'][self.doc_type]['_all']
    assert all_field_mapping["enabled"] == False

  def test_create_index_with_best_compression(self):
    self.client.delete_index(TEST_INDEX)
    self.prepare_indices._create_index_with_best_compression(TEST_INDEX)
    settings = self.client.get_settings(index=TEST_INDEX)[TEST_INDEX]["settings"]['index']
    assert settings["codec"] == "best_compression"

  def test_max_result_size(self):
    self.prepare_indices._set_max_result_size(TEST_INDEX)
    settings = self.client.get_settings(index=TEST_INDEX)[TEST_INDEX]["settings"]['index']
    assert settings["max_result_window"] == '100000'

  def test_index_exists(self):
    index_exists = self.prepare_indices._index_exists(TEST_INDEX)
    self.client.delete_index(TEST_INDEX)
    index_not_exists = self.prepare_indices._index_exists(TEST_INDEX)
    assert index_exists
    assert not index_not_exists

  def test_prepare_fast_index(self):
    mockify(self.prepare_indices, {
      "_index_exists": MagicMock(return_value=False)
    }, "_prepare_fast_index")

    self.prepare_indices._prepare_fast_index(TEST_INDEX, self.doc_type)

    self.prepare_indices._index_exists.assert_called_with(TEST_INDEX)
    for method in self.index_methods:
      getattr(self.prepare_indices, method).assert_called_with(TEST_INDEX)
    for method in self.mapping_methods:
      getattr(self.prepare_indices, method).assert_called_with(TEST_INDEX, self.doc_type)

  def test_prepare_fast_non_empty_index(self):
    mockify(self.prepare_indices, {
      "_index_exists": MagicMock(return_value=True)
    }, "_prepare_fast_index")
    self.prepare_indices._prepare_fast_index(TEST_INDEX, self.doc_type)
    for method in self.index_methods + self.mapping_methods:
      getattr(self.prepare_indices, method).assert_not_called()

  def test_prepare_index(self):
    self.prepare_indices._index_exists = MagicMock(return_value=False)
    self.prepare_indices.client.create_index = MagicMock()

    self.prepare_indices._prepare_index(TEST_INDEX)

    self.prepare_indices._index_exists.assert_called_with(TEST_INDEX)
    self.prepare_indices.client.create_index.assert_called_with(TEST_INDEX)

  def test_prepare_non_empty_index(self):
    self.prepare_indices._index_exists = MagicMock(return_value=True)
    self.prepare_indices.client.create_index = MagicMock()

    self.prepare_indices._prepare_index(TEST_INDEX)

    self.prepare_indices.client.create_index.assert_not_called()

  def test_prepare_indices(self):
    self.prepare_indices._prepare_index = MagicMock()
    self.prepare_indices._prepare_fast_index = MagicMock()

    self.prepare_indices.prepare_indices()

    self.prepare_indices._prepare_fast_index.assert_any_call(TEST_INDICES["internal_transaction"], "itx")
    for key in TEST_INDICES:
      if key != "internal_transaction":
        self.prepare_indices._prepare_index.assert_any_call(TEST_INDICES[key])


  def _get_elasticsearch_size(self):
    result = subprocess.run(["du", "-sb", "/var/lib/elasticsearch"], stdout=subprocess.PIPE)
    return int(result.stdout.split()[0])

  def _add_records(self, doc, number=10000, iterations=1):
    for _ in range(iterations):
      docs = [{**doc, **{"id": i + 1}} for i in range(0, number)]
      self.client.bulk_index(index=TEST_INDEX, doc_type=self.doc_type, docs=docs, refresh=True)

  def xtest_real_max_result_size(self):
    self.new_client._set_max_result_size(TEST_INDEX, 10)
    self._add_records({'test': 1}, number=10)
    with self.assertRaises(Exception):
      self._add_records({'test': 1}, number=1)

  def xtest_fast_index_size(self):
    self._add_records(self.doc)
    size_before = self._get_elasticsearch_size()

    self.client.delete_index(TEST_INDEX)
    self.new_client.prepare_fast_index(TEST_INDEX, doc_type=self.doc_type)
    self._add_records(self.doc)
    size_after = self._get_elasticsearch_size()    

    compression = size_after / size_before
    print("Compression: {:.1%}".format(compression))
    print("Current size: {:.1f}".format(CURRENT_ELASTICSEARCH_SIZE / (1024 ** 3)))
    print("Compressed size: {:.1f}".format(compression * CURRENT_ELASTICSEARCH_SIZE / (1024 ** 3)))
    assert size_after < size_before

  def xtest_fast_index_speed(self):
    start_time = time()
    self._add_records(self.doc)
    end_time = time()
    common_index_time = end_time - start_time

    self.client.delete_index(TEST_INDEX)
    self.new_client.prepare_fast_index(TEST_INDEX, doc_type=self.doc_type)

    start_time = time()
    self._add_records(self.doc)
    end_time = time()    
    fast_index_time = end_time - start_time

    boost = fast_index_time / common_index_time
    print("Time boost: {:.1%}".format(boost))
    assert fast_index_time < common_index_time

class ClickhouseIndicesTestCase(unittest.TestCase):
  _test_blocks = [{
    "number": 100,
    "timestamp": datetime.now()
  }]
  _test_transactions = {
    "call_staticcall": {
      "hash": 1,
      "blockHash": "0x901414b33ccb50712e82bc238b42ff376791991bee9e1d8281a002ff33962d8b",
      "traceAddress": [
        0
      ],
      "type": "call",
      "transactionHash": "0xc9f7a5e451e2dc020edf7bbd56b2e6e5c7ae4c5b69ef69ad875204ab47393a43",
      "callType": "staticcall",
      "output": "0x0000000000000000000000000000000000000000000000000000000000000000",
      "input": "0x70a08231000000000000000000000000bda109309f9fafa6dd6a9cb9f1df4085b27ee8ef",
      "gasUsed": "0x32f",
      "transactionPosition": 133,
      "blockNumber": 5846858,
      "gas": "0xd0110",
      "from": "0x4b79366182ddd0dce4b1282498ffcc76dc37668e",
      "to": "0xf53ad2c6851052a81b42133467480961b2321c09",
      "value": 0,
      "subtraces": 0
    },
    "call_delegatecall": {
      "hash": 2,
      "blockHash": "0x3b7009b18a7393ef32cc75fbbffb5d2c2c3aaa132abf5945931614b8eea01a99",
      "traceAddress": [
        1,
        2
      ],
      "type": "call",
      "transactionHash": "0xd1158157001d93ddaacd7c29636b8dd37098197ebaaa1253140c6c97cf531951",
      "callType": "delegatecall",
      "output": "0x0000000000000000000000000000000000000000000000000000000000000001",
      "input": "0x32921690000000000000000000000000606ddac6f2928369e8515340f8de97fe2d1667770000000000000000000000000000000000000000000000000000000000000001",
      "gasUsed": "0x51c",
      "transactionPosition": 8,
      "blockNumber": 3147251,
      "gas": "0x1b30f",
      "from": "0x68c769478002b2e2db64fe3be55c943fe4fbd6b1",
      "to": "0x606ddac6f2928369e8515340f8de97fe2d166777",
      "value": 0,
      "subtraces": 1
    },
    "call_call": {
      "hash": 3,
      "blockHash": "0xf6e13968e302df7c24793726ac065e00e06b87d1bdf8cd8ea62324b99aa45f99",
      "traceAddress": [
        0
      ],
      "type": "call",
      "transactionHash": "0x697b1025835345460283b5cf3615149602ead6249c5938c9678374559f198130",
      "callType": "call",
      "output": "0x0000000000000000000000000000000000000000000000000000000000000000",
      "input": "0x524f38890000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000355524c0000000000000000000000000000000000000000000000000000000000",
      "gasUsed": "0x534",
      "transactionPosition": 30,
      "blockNumber": 3609421,
      "gas": "0x2687f",
      "from": "0xa545da30f0a7cb1d3eafa7e4add768d66a993be1",
      "to": "0x001a589dda0d6be37632925eaf1256986b2c6ad0",
      "value": 0,
      "subtraces": 0
    },
    "call_callcode": {
      "hash": 4,
      "blockHash": "0x0820cb73cede6496d1b5ac2417e2c110fd1f24f63c5cbdc78f3d444c8bc847b0",
      "traceAddress": [
        0
      ],
      "type": "call",
      "transactionHash": "0x5171bcff43bd52878f6d4bf9767f7a37dd32725808deb9c162ad532eaa811a74",
      "callType": "callcode",
      "output": "0x0000000000000000000000000000000000000000000000000000000000000000",
      "input": "0x5035db4a0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000caa216e03ee4932941ef0729f250e297fd5655ad",
      "gasUsed": "0x255",
      "transactionPosition": 1,
      "blockNumber": 3241777,
      "gas": "0xae05",
      "from": "0xfdc77b9cb732eb8c896b152e28294521f5f62e67",
      "to": "0xfdc77b9cb732eb8c896b152e28294521f5f62e67",
      "value": 0,
      "subtraces": 0
    },
    "create": {
      "hash": 5,
      "blockHash": "0x5ce9b8599ad120469b35ca06f597ab89e558ce65c07dbd6c48e95987ba2cd050",
      "init": "0x6060604052346100005760405161136138038061136183398101604090815281516020830151918301519201915b805b83835b815160019081019055600033600160a060020a03166003825b505550600160a060020a033316600090815261010260205260408120600190555b82518110156100ee57828181518110156100005790602001906020020151600160a060020a0316600282600201610100811015610000570160005b5081905550806002016101026000858481518110156100005790602001906020020151600160a060020a03168152602001908152602001600020819055505b60010161006c565b60008290555b5050506101058190556101126401000000006111f161012182021704565b610107555b505b50505061012b565b6201518042045b90565b6112278061013a6000396000f300606060405236156100e05763ffffffff60e060020a600035041663173825d981146101365780632f54bf6e146101515780634123cb6b1461017e578063523750931461019d5780635c52c2f5146101bc578063659010e7146101cb5780637065cb48146101ea578063746c917114610205578063797af62714610224578063b20d30a914610248578063b61d27f61461025a578063b75c7dc614610295578063ba51a6df146102a7578063c2cf7326146102b9578063c41a360a146102e9578063cbf0b0c014610315578063f00d4b5d14610330578063f1736d8614610351575b6101345b60003411156101315760408051600160a060020a033316815234602082015281517fe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c929181900390910190a15b5b565b005b3461000057610134600160a060020a0360043516610370565b005b346100005761016a600160a060020a036004351661045d565b604080519115158252519081900360200190f35b346100005761018b61047e565b60408051918252519081900360200190f35b346100005761018b610484565b60408051918252519081900360200190f35b346100005761013461048b565b005b346100005761018b6104c3565b60408051918252519081900360200190f35b3461000057610134600160a060020a03600435166104ca565b005b346100005761018b6105b9565b60408051918252519081900360200190f35b346100005761016a6004356105bf565b604080519115158252519081900360200190f35b34610000576101346004356108a7565b005b346100005761018b60048035600160a060020a03169060248035916044359182019101356108e0565b60408051918252519081900360200190f35b3461000057610134600435610aa3565b005b3461000057610134600435610b4e565b005b346100005761016a600435600160a060020a0360243516610bd0565b604080519115158252519081900360200190f35b34610000576102f9600435610c25565b60408051600160a060020a039092168252519081900360200190f35b3461000057610134600160a060020a0360043516610c45565b005b3461000057610134600160a060020a0360043581169060243516610c83565b005b346100005761018b610d7c565b60408051918252519081900360200190f35b600060003660405180838380828437820191505092505050604051809103902061039981610d83565b1561045657600160a060020a0383166000908152610102602052604090205491508115156103c657610456565b60016001540360005411156103da57610456565b6000600283610100811015610000570160005b5055600160a060020a03831660009081526101026020526040812055610411610f1e565b610419610fee565b60408051600160a060020a038516815290517f58619076adf5bb0943d100ef88d52d7c3fd691b19d3a9071b555b651fbf418da9181900360200190a15b5b5b505050565b600160a060020a03811660009081526101026020526040812054115b919050565b60015481565b6101075481565b6000366040518083838082843782019150509250505060405180910390206104b281610d83565b156104be576000610106555b5b5b50565b6101065481565b6000366040518083838082843782019150509250505060405180910390206104f181610d83565b156105b3576104ff8261045d565b15610509576105b3565b610511610f1e565b60015460fa901061052457610524610fee565b5b60015460fa9010610535576105b3565b60018054810190819055600160a060020a03831690600290610100811015610000570160005b5055600154600160a060020a03831660008181526101026020908152604091829020939093558051918252517f994a936646fe87ffe4f1e469d3d6aa417d6b855598397f323de5b449f765f0c3929181900390910190a15b5b5b5050565b60005481565b60006000826105cd81610d83565b1561089d5760008481526101086020526040902054600160a060020a03161561089d5760008481526101086020526040908190208054600180830154935160029384018054600160a060020a0390941695949093919283928592918116156101000260001901160480156106825780601f1061065757610100808354040283529160200191610682565b820191906000526020600020905b81548152906001019060200180831161066557829003601f168201915b505091505060006040518083038185876185025a03f19250505091507fe7c957c06e9a662c1a6c77366179f5b702b97651dc28eee7d5bf1dff6e40bb4a338561010860008860001916600019168152602001908152602001600020600101546101086000896000191660001916815260200190815260200160002060000160009054906101000a9004600160a060020a031661010860008a600019166000191681526020019081526020016000206002016040518086600160a060020a0316600160a060020a03168152602001856000191660001916815260200184815260200183600160a060020a0316600160a060020a03168152602001806020018281038252838181546001816001161561010002031660029004815260200191508054600181600116156101000203166002900480156108005780601f106107d557610100808354040283529160200191610800565b820191906000526020600020905b8154815290600101906020018083116107e357829003601f168201915b5050965050505050505060405180910390a16000848152610108602052604081208054600160a060020a0319168155600180820183905560028083018054858255939493909281161561010002600019011604601f8190106108625750610894565b601f01602090049060005260206000209081019061089491905b80821115610890576000815560010161087c565b5090565b5b505050600192505b5b5b5b5050919050565b6000366040518083838082843782019150509250505060405180910390206108ce81610d83565b156105b3576101058290555b5b5b5050565b60006108eb3361045d565b15610a985760003643604051808484808284378201915050828152602001935050505060405180910390209050610921816105bf565b158015610944575060008181526101086020526040902054600160a060020a0316155b15610a985760008181526101086020908152604082208054600160a060020a038916600160a060020a0319909116178155600180820188905560029182018054818652948490209094601f928116156101000260001901169290920481019290920481019185919087908390106109c65782800160ff198235161785556109f3565b828001600101855582156109f3579182015b828111156109f35782358255916020019190600101906109d8565b5b50610a149291505b80821115610890576000815560010161087c565b5090565b505060408051828152600160a060020a033381811660208401529282018790528716606082015260a0608082018181529082018590527f1733cbb53659d713b79580f79f3f9ff215f78a7c7aa45890f3b89fc5cddfbf32928492909188918a918991899160c082018484808284376040519201829003995090975050505050505050a15b5b5b5b949350505050565b600160a060020a033316600090815261010260205260408120549080821515610acb57610b47565b50506000828152610103602052604081206001810154600284900a929083161115610b475780546001908101825581018054839003905560408051600160a060020a03331681526020810186905281517fc7fb647e59b18047309aa15aad418e5d7ca96d173ad704f1031a2c3d7591734b929181900390910190a15b5b50505050565b600036604051808383808284378201915050925050506040518091039020610b7581610d83565b156105b357600154821115610b89576105b3565b6000829055610b96610f1e565b6040805183815290517facbdb084c721332ac59f9b8e392196c9eb0e4932862da8eb9beaf0dad4f550da9181900360200190a15b5b5b5050565b600082815261010360209081526040808320600160a060020a038516845261010290925282205482811515610c085760009350610c1c565b8160020a9050808360010154166000141593505b50505092915050565b6000600282600101610100811015610000570160005b505490505b919050565b600036604051808383808284378201915050925050506040518091039020610c6c81610d83565b156105b35781600160a060020a0316ff5b5b5b5050565b6000600036604051808383808284378201915050925050506040518091039020610cac81610d83565b15610b4757610cba8361045d565b15610cc457610b47565b600160a060020a038416600090815261010260205260409020549150811515610cec57610b47565b610cf4610f1e565b82600160a060020a0316600283610100811015610000570160005b5055600160a060020a0380851660008181526101026020908152604080832083905593871680835291849020869055835192835282015281517fb532073b38c83145e3e5135377a08bf9aab55bc0fd7c1179cd4fb995d2a5159c929181900390910190a15b5b5b50505050565b6101055481565b600160a060020a033316600090815261010260205260408120548180821515610dab57610f14565b60008581526101036020526040902080549092501515610e3f576000805483556001808401919091556101048054918201808255828015829011610e1457600083815260209020610e149181019083015b80821115610890576000815560010161087c565b5090565b5b50505060028301819055610104805487929081101561000057906000526020600020900160005b50555b8260020a90508082600101541660001415610f145760408051600160a060020a03331681526020810187905281517fe1c52dc63b719ade82e8bea94cc41a0d5d28e4aaf536adb5e9cccc9ff8c1aeda929181900390910190a1815460019011610f015760008581526101036020526040902060020154610104805490919081101561000057906000526020600020900160005b506000908190558581526101036020526040812081815560018082018390556002909101919091559350610f14565b8154600019018255600182018054821790555b5b5b505050919050565b6101045460005b81811015610fe157610108600061010483815481101561000057906000526020600020900160005b50548152602081019190915260400160009081208054600160a060020a0319168155600180820183905560028083018054858255939493909281161561010002600019011604601f819010610fa25750610fd4565b601f016020900490600052602060002090810190610fd491905b80821115610890576000815560010161087c565b5090565b5b5050505b600101610f25565b6105b361111d565b5b5050565b60015b6001548110156104be575b6001548110801561101e5750600281610100811015610000570160005b505415155b1561102b57600101610ffc565b5b600160015411801561105057506002600154610100811015610000570160005b5054155b15611064576001805460001901905561102c565b6001548110801561108857506002600154610100811015610000570160005b505415155b80156110a45750600281610100811015610000570160005b5054155b15611114576002600154610100811015610000570160005b5054600282610100811015610000570160005b5055806101026000600283610100811015610000570160005b505481526020019081526020016000208190555060006002600154610100811015610000570160005b50555b610ff1565b5b50565b6101045460005b8181101561119a5761010481815481101561000057906000526020600020900160005b50541561119157610103600061010483815481101561000057906000526020600020900160005b505481526020810191909152604001600090812081815560018101829055600201555b5b600101611124565b6101048054600080835591909152610456907f4c0be60200faa20559308cb7b5a1bb3255c16cb1cab91f525b5ae7a03d02fabe908101905b80821115610890576000815560010161087c565b5090565b5b505b5050565b6201518042045b905600a165627a7a72305820f0fdc796f510b51e8b2e979ceb3cecb8cc5ce8eee4fdb2e1256c0c2f9a329ef400290000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "code": "0x",
      "traceAddress": [],
      "error": "Out of gas",
      "type": "create",
      "transactionHash": "0xdf3e180ba2a69002230655c8a2b568b855cfdf250b225c581df5acc470cde35e",
      "transactionPosition": 0,
      "blockNumber": 3510702,
      "gas": "0x6134",
      "from": "0xd8213dd9e21626f45b4e28b85e15f90bc11cd2fe",
      "value": 0,
      "subtraces": 0
    },
    "suicide": {
      "result": None,
      "hash": 6,
      "refundAddress": "0x6d28515bf27529843f14dc75cc7ee95a4783e3a1",
      "blockHash": "0xffe458f4eb5fddb9c5446e6f5aef2344644e91db13dc7c53aeaebfb9d03efb67",
      "address": "0x0dad1d600347bbb5f8b93a5a1017a5cfc07120db",
      "transactionPosition": 22,
      "balance": "0x0",
      "traceAddress": [
        0
      ],
      "blockNumber": 3354839,
      "type": "suicide",
      "transactionHash": "0x69ca817e02df5c4c219863296a2af4e2966b22ec14096e8bc7e1e359fc72997e",
      "subtraces": 0
    },
    "reward_block": {
      "hash": 7,
      "traceAddress": [],
      "blockHash": "0x57c32178208f544dba7eb8696ba4f7132537c854a74bc303cb335502e3d94b02",
      "rewardType": "block",
      "type": "reward",
      "subtraces": 0,
      "author": "0x3dfcf2bf579c831a03379a140b74f884e49caff3",
      "value": 5,
      "blockNumber": 51259,
    },
    "reward_uncle": {
      "hash": 8,
      "blockHash": "0xcaf341d0bc00339b91cd09a914777b9b3eae79d2c40053609a0fa321035e8428",
      "value": 3.75,
      "type": "reward",
      "subtraces": 0,
      "traceAddress": [],
      "author": "0x2a65aca4d5fc5b5c859090a6c34d164135398226",
      "blockNumber": 1102454,
      "rewardType": "uncle"
    }
  }

  def setUp(self):
    self.client = TestClickhouse()
    self.client.send_sql_request("DROP TABLE IF EXISTS {}".format(TEST_INDEX))
    for index in TEST_INDICES.values():
      self.client.send_sql_request("DROP TABLE IF EXISTS {}".format(index))
    self.indices = ClickhouseIndices(TEST_INDICES)

  def test_create_index_with_id(self):
    self.indices._create_index(TEST_INDEX)
    self.client.bulk_index(index=TEST_INDEX, docs=[{"id": 1}, {"id": 2}])
    result = self.client.search(index=TEST_INDEX, query=None, fields=[])
    result = [item["_id"] for item in result]
    self.assertCountEqual(["1", "2"], result)

  def test_create_replacement_index(self):
    self.indices._create_index(TEST_INDEX, {"x": "Int32"})
    self.client.bulk_index(index=TEST_INDEX, docs=[{"id": 1, "x": 10}, {"id": 1, "x": 11}, {"id": 2, "x": 12}])
    result = self.client.search(index=TEST_INDEX, query=None, fields=["x"])
    result = [(item["_id"], item["_source"]["x"]) for item in result]
    self.assertCountEqual([('1', 11), ('2', 12)], result)

  def test_create_index_if_not_exists(self):
    self.client.send_sql_request("CREATE TABLE {} (id String) ENGINE = MergeTree() ORDER BY id".format(TEST_INDEX))
    self.indices._create_index(TEST_INDEX)

  def test_create_blocks_index(self):
    self.indices.prepare_indices()
    self.client.bulk_index(index=TEST_INDICES["block"], docs=self._test_blocks, id_field="number")
    result = self.client.search(index=TEST_INDICES["block"], fields=[])
    result = [block["_id"] for block in result]
    self.assertCountEqual(["100"], result)

  def test_create_internal_transaction_index(self):
    self.indices.prepare_indices()
    all_fields = list(set([field for doc in self._test_transactions.values() for field in doc.keys()]))
    nullable_fields = []
    for transaction in self._test_transactions.values():
      nullable_fields += [k for k in all_fields if k not in transaction]
    self.client.bulk_index(index=TEST_INDICES["internal_transaction"], docs=list(self._test_transactions.values()), id_field="hash")
    result = self.client.search(index=TEST_INDICES["internal_transaction"], fields=[])
    result = [transaction["_id"] for transaction in result]
    self.assertCountEqual(result, [str(i + 1) for i in range(len(self._test_transactions))])

  def test_create_block_traces_extracted_index(self):
    self.indices.prepare_indices()
    self.client.bulk_index(index=TEST_INDICES["block_flag"], docs=[
      {"id": 1, "name": "traces_extracted", "value": True},
      {"id": 2, "name": "traces_extracted", "value": None},
      {"id": 1, "name": "something_else", "value": True},
    ])
    result = self.client.search(index=TEST_INDICES["block_flag"], fields=[])
    result = [flag["_id"] for flag in result]
    self.assertCountEqual(["1", "2", "1"], result)

  def test_create_contract_abi_index(self):
    self.indices.prepare_indices()
    self.client.bulk_index(index=TEST_INDICES["contract_abi"],
                           docs=[{"id": 1, "abi_extracted": True}, {"id": 2, "abi_extracted": None}, {"id": 3, "abi": {"test": 1}, "abi_extracted": True}])
    result = self.client.search(index=TEST_INDICES["contract_abi"], fields=[])
    result = [flag["_id"] for flag in result]
    self.assertCountEqual(["1", "2", "3"], result)

  def test_create_contract_block_index(self):
    self.indices.prepare_indices()
    self.client.bulk_index(index=TEST_INDICES["contract_block"], docs=[
      {"id": 1, "name": "flag_1", "value": 1234},
      {"id": 1, "name": "flag_2", "value": 1235},
    ])
    result = self.client.search(index=TEST_INDICES["contract_block"], fields=[])
    result = [flag["_id"] for flag in result]
    self.assertCountEqual(["1", "1"], result)

  def test_create_transaction_fee_index(self):
    self.indices.prepare_indices()
    self.client.bulk_index(index=TEST_INDICES["transaction_fee"], docs=[{"id": "0x1", "gasUsed": 2100, "gasPrice": 20.12}])
    result = self.client.search(index=TEST_INDICES["transaction_fee"], fields=[])
    result = [flag["_id"] for flag in result]
    self.assertCountEqual(["0x1"], result)

  def test_create_events_index(self):
    self.indices.prepare_indices()
    test_event = {
      'type': 'mined',
      'logIndex': 0,
      'transactionLogIndex': 0,
      'data': '0x000000000000000000000000000000000000000000000b3cb19896ad16d0c000',
      'transactionIndex': 2,
      'address': '0x0f5d2fb29fb7d3cfee444a200298f468908cc942',
      'transactionHash': '0x93159c656e7a4c11624b7935eb507125cf82f1aae9694fbacf5470bed7d84772',
      'blockHash': '0x43340a6d232532c328211d8a8c0fa84af658dbff1f4906ab7a7d4e41f82fe3a3',
      'blockNumber': 4500000,
      'id': '0x1',
      'topics': ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '0x0000000000000000000000004d468cf47eb6df39618dc9450be4b56a70a520c1', '0x000000000000000000000000915c0d974fef3593444028a232fda420fd6e9d1a']
    }
    self.client.bulk_index(index=TEST_INDICES["event"], docs=[test_event])
    result = self.client.search(index=TEST_INDICES["event"], fields=[])
    result = [flag["_id"] for flag in result]
    self.assertCountEqual(["0x1"], result)

CURRENT_ELASTICSEARCH_SIZE = 290659165119
TEST_INDEX = 'test_ethereum_transactions'
TEST_INDICES = {
  "contract": "test_ethereum_contract",
  "transaction": "test_ethereum_transaction",
  "internal_transaction": "test_ethereum_internal_transaction",
  "listed_token": "test_ethereum_listed_token",
  "token_tx": "test_ethereum_token_transaction",
  "block": "test_ethereum_block",
  "miner_transaction": "test_ethereum_miner_transaction",
  "token_price": "test_ethereum_token_price",
  "event": "test_ethereum_event",

  "block_flag": "test_block_traces_extracted",
  "contract_abi": "test_contract_abi",
  "contract_block": "test_contract_block",
  "transaction_fee": "test_transaction_fee"
}