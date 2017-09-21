import datetime
import logging
import psycopg2
from datastore import Datastore
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy


class PostgreSQLDatastore(Datastore):

    TX_INDEX_NAME = "ethereum-transaction"
    B_INDEX_NAME = "ethereum-block"

    NAME_TABLE_TRANSACTIONS = 'ethereumTransactions'
    NAME_TABLE_BLOCKS = 'ethereumBlocks'
    DELTA_BLOCKS = 100000

    def __init__(self):
        super().__init__()
        self.postgres = create_engine(self.postgres_url)

    @classmethod
    def config(cls, postgres_url):
        cls.postgres_url = postgres_url

    def extract(self, rpc_block):
        block = rpc_block["result"]

        transactions = block["transactions"]
        tx_hashes = list()
        tx_value_sum = 0

        block_nb = int(block["number"], 0)
        block_timestamp = datetime.datetime.fromtimestamp(int(block["timestamp"], 0))

        for tx in transactions:
            tx["blockNumber"] = block_nb
            tx["blockTimestamp"] = block_timestamp
            # Convert wei into ether
            tx["value"] = int(tx["value"], 0) / self.WEI_ETH_FACTOR
            tx_value_sum += tx["value"]
            self.actions.append(
                {"_index": self.TX_INDEX_NAME, "_type": "tx", "_id": tx["hash"], "_source": tx}
            )
            tx_hashes.append(tx["hash"])

        block["transactions"] = tx_hashes
        block["number"] = block_nb
        block["timestamp"] = block_timestamp
        block["gasLimit"] = int(block["gasLimit"], 0)
        block["gasUsed"] = int(block["gasUsed"], 0)
        block["size"] = int(block["size"], 0)
        block["transactionCount"] = len(tx_hashes)
        block["txValueSum"] = tx_value_sum
        block["_index"] = self.B_INDEX_NAME
        block["_type"] = "b"
        block["_id"] = block_nb

        self.actions.append(
            block
        )

    def save(self):
        nb_blocks = sum(act["_type"] == "b" for act in self.actions)
        nb_txs = sum(act["_type"] == "tx" for act in self.actions)

        if self.actions:
            try:
                for item in self.actions:
                    if item['_type'] == 'tx':
                        transactions_df = pd.DataFrame(item['_source'], index=[0])
                        transactions_df.columns = [c.lower().replace(' ', '') for c in transactions_df.columns]
                        transactions_df['gasprice']         = int(str(transactions_df['gasprice'])[5:-30], 16)
                        transactions_df['gas']              = int(str(transactions_df['gas'])[5:-25], 16)
                        transactions_df['transactionindex'] = int(str(transactions_df['transactionindex'])[5:-22 - len('transactionindex')], 16)
                        transactions_df['v']                = int(str(transactions_df['v'])[5:-22 - len('v')], 16)

                        try:
                            transactions_df_collect = transactions_df_collect.append(transactions_df)
                        except UnboundLocalError:
                            transactions_df_collect = transactions_df

                    elif item['_type'] == 'b':
                        blocks_df = pd.DataFrame({k:v for k,v in item.items() if k not in ('uncles', 'sealFields', 'transactions')}, index=[0])
                        blocks_df.columns = [c.lower().replace(' ', '') for c in blocks_df.columns]

                        try:
                            blocks_df_collect = blocks_df_collect.append(blocks_df)
                        except UnboundLocalError:
                            blocks_df_collect = blocks_df


                try:
                    if (not transactions_df_collect.empty): transactions_df_collect.to_sql(self.NAME_TABLE_TRANSACTIONS, self.postgres, if_exists='append')
                except (UnboundLocalError, ValueError):
                    pass

                blocks_df_collect.to_sql(self.NAME_TABLE_BLOCKS, self.postgres, if_exists='append')

                return "{} blocks and {} transactions indexed in PostgreSQL".format(
                    nb_blocks, nb_txs
                )

            except sqlalchemy.exc.ProgrammingError as exception:
                print("Issue with {} blocks:\n{}\n".format(nb_blocks, exception))
                blocks = (act for act in self.actions if act["_type"] == "b")
                for block in blocks:
                    logging.error("block: " + str(block["_id"]))

    @staticmethod
    def request(url,**kwargs):
        engine = create_engine(url)
        result = engine.execute(**kwargs)
        for row in result: query_result = row[0]
        result.close()
        return query_result

    @staticmethod
    def start_block(url):
        max_block_number_in_table = PostgreSQLDatastore.request(url,
                                    statement='SELECT max(_id) as "max_block_number"  FROM public."{}"'.format(
                                        PostgreSQLDatastore.NAME_TABLE_BLOCKS))
        count_blocks_in_table = PostgreSQLDatastore.request(url,
                                    statement='SELECT count(_id) as "count_blocks"  FROM public."{}"'.format(
                                        PostgreSQLDatastore.NAME_TABLE_BLOCKS))
        for i in range(int(count_blocks_in_table/PostgreSQLDatastore.DELTA_BLOCKS)):
            if max_block_number_in_table + 1 == count_blocks_in_table:
                return  max_block_number_in_table
            else:
                count_blocks_in_table -= PostgreSQLDatastore.DELTA_BLOCKS
                max_block_number_in_table = PostgreSQLDatastore.request(url,
                                                                        statement='SELECT max(_id) as "max_block_number"  FROM public."{}" WHERE _id <= {}'.format(
                                                                            PostgreSQLDatastore.NAME_TABLE_BLOCKS, count_blocks_in_table))
        return 0

    @staticmethod
    def delete_replacement_rows(url, start_block):
        try:
            PostgreSQLDatastore.request(url,
                                    statement='DELETE * FROM public."{}" WHERE _id >= {}'.format(
                                        PostgreSQLDatastore.NAME_TABLE_BLOCKS, start_block))
            PostgreSQLDatastore.request(url,
                                    statement='DELETE * FROM public."{}" WHERE blocknumber >= {}'.format(
                                        PostgreSQLDatastore.NAME_TABLE_TRANSACTIONS, start_block))
            return 1

        except (sqlalchemy.exc.ProgrammingError, psycopg2.ProgrammingError):
            return 0

