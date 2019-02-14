from clients.custom_clickhouse import CustomClickhouse
from config import INDICES
from web3 import Web3


class ClickhouseContractTransactions:
    def __init__(self, indices=INDICES):
        self.indices = indices
        self.client = CustomClickhouse()

    def _extract_first_bytes(self, func):
        '''
        Create contract method signature and return first 4 bytes of this signature

        Parameters
        ----------
        func: str
          String that contains function name and arguments

        Returns
        -------
        str
          String with first 4 bytes of method signature in hex format
        '''
        return str(Web3.toHex(Web3.sha3(text=func)[0:4]))[2:]

    def _extract_methods_signatures(self):
        '''
        Return dictionary with first bytes of standard method signatures

        Returns
        -------
        dict
          Dictionary with first 4 bytes of methods signatures in hex format
        '''
        return {
            'erc20': {
                'totalSupply': self._extract_first_bytes('totalSupply()'),
                'balanceOf': self._extract_first_bytes('balanceOf(address)'),
                'allowance': self._extract_first_bytes('allowance(address,address)'),
                'transfer': self._extract_first_bytes('transfer(address,uint256)'),
                'transferFrom': self._extract_first_bytes('transferFrom(address,address,uint256)'),
                'approve': self._extract_first_bytes('approve(address,uint256)'),
            },
            'erc223': {
                'tokenFallback': self._extract_first_bytes('tokenFallback(address,uint256,bytes)')
            }
        }

    def _get_standards(self):
        standards = self._extract_methods_signatures()
        return {
            "standard_" + standard: " AND ".join([
                "(bytecode LIKE '%{}%')".format(signature) for signature in signatures.values()
            ])
            for standard, signatures in standards.items()
        }

    def _get_fields(self):
        standard_fields = self._get_standards()
        fields = {
            "id": "coalesce(address, id)",
            "blockNumber": "blockNumber",
            "address": "address",
            "owner": "from",
            "bytecode": "code"
        }
        fields.update(standard_fields)
        fields_string = ", ".join([
            "{} AS {}".format(field, alias)
            for alias, field in fields.items()
        ])
        return fields_string

    def extract_contract_addresses(self):
        fields_string = self._get_fields()
        engine_string = 'ENGINE = ReplacingMergeTree() ORDER BY id'
        condition = "type = 'create' AND error IS NULL AND parent_error IS NULL"
        sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS {} {} POPULATE AS (SELECT {} FROM {} WHERE {})".format(
            self.indices["contract"],
            engine_string,
            fields_string,
            self.indices["internal_transaction"],
            condition
        )
        self.client.send_sql_request(sql)
