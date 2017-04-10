# Ethdrain

Python 3 script allowing to copy and index the Ethereum blockchain in an efficient way to ElasticSearch by connecting to a local node supporting RPC (tried with Parity).

I hardcoded the use of Elasticsearch but feel free to fork it to support others.

Pull requests are welcome!

As of now, this tool saves all block data as well as the related transaction data. The relation is kept as follows: 
* "ethereum-block" documents have:
    * a "transactionCount" property, with the count of their respective transactions.
    * a "transactions" property, with is an "array" of transaction hashes.
* "ethereum-transaction" documents have:
    * a "blockNumber" property, refering to their parent block.
    * a "blockTimestamp" property, refering to their parent block's timestamp.

The value of a transaction is stored in ether, not in wei and a sum is available at block level ("txValueSum" field).

The following fields are converted from their hex value to a real number:
* Block number
* Gas limit
* Gas used
* Size
* Transaction value

I was able to download the entire blockchain (3'400'000 block approx.) using this tool. 

You can customize most of the useful parameters by tweaking the constants in the script:
```python
# Elasticsearch maximum number of connections
ES_MAXSIZE = 10
# Parallel processing semaphore size
SEM_SIZE   = 256
# Size of chunk size in blocks
CHUNK_SIZE = 500
# Size of multiprocessing Pool processing the chunks
POOL_SIZE  = 8
```

## Usage
```bash
>  ./ethdrain.py -h
usage: ethdrain.py [-h] [-s START_BLOCK] [-e END_BLOCK] [-f FILE] [-u ESURL]
                   [-m ESMAXSIZE] [-r ETHRPCURL]

optional arguments:
  -h, --help            show this help message and exit
  -s START_BLOCK, --start START_BLOCK
                        What block to start indexing. If nothing is provided,
                        the lastest block indexed in ElasticSearch will be
                        used.
  -e END_BLOCK, --end END_BLOCK
                        What block to finish indexing. If nothing is provided,
                        the latest one will be used.
  -f FILE, --file FILE  Use an input file, each block number on a new line.
  -u ESURL, --esurl ESURL
                        The elasticsearch url and port. Accepts all the same
                        parameters needed as a normal Elasticsearch client
                        expects.
  -m ESMAXSIZE, --esmaxsize ESMAXSIZE
                        The elasticsearch max chunk size.
  -r ETHRPCURL, --ethrpcurl ETHRPCURL
                        The Ethereum RPC node url and port.
```

## Benchmarks
Few benchmarks of copying blocks into ElasticSearch as well as all related transactions on an Intel i7-6700 @ 4GHz and 32GB of RAM. The POOL\_SIZE parameter was set to 12.

| # | start block | end block | # of blocks | time taken (in minutes) |
|---|-------------|-----------|-------------|-------------------------|
| 1 |           0 |   500'000 |     500'000 |                      16 |
| 2 |     500'000 | 2'000'000 |   1'500'000 |                      87 |
| 3 |   2'000'000 | 3'000'000 |   1'000'000 |                      60 |
| 4 |   3'000'000 | 3'475'450 |     475'440 |                      39 |

## Planned features
* Indexing of addresses / contracts
* Automatically get the last current block (as default param)
* Continuous sync of blocks and transactions

## To-do
* Friendly help/doc inside the script
* Include some unit-testing
* Better architecture to add other databases easily

