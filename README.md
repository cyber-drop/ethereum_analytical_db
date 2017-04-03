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

## Use
You can use it either with one parameter, a list of block numbers (separated by a new line), or with two integers representing a range.
In case of an expected error, it will print new faulty block numbers to stderr.

```bash
# Simple range
> ./ethdrain.py <start_block_nb> <end_block_nb>

# Output faulty blocks to file
> ./ethdrain.py 0 1000000 2> error_blocks

# Input a list of faulty block for retry
> ./ethdrain.py error_blocks 2> new_errors
```

## Benchmarks
Few benchmarks of copying blocks into ElasticSearch as well as all related transactions on an average computer (ES node is not running on the same machine):

| # | start block | end block | # of blocks | time taken (in minutes) |
|---|-------------|-----------|-------------|-------------------------|
| 1 |           0 | 1'000'000 |   1'000'000 |                      40 |
| 2 |   1'000'000 | 2'000'000 |   1'000'000 |                      90 |
| 3 |   3'451'780 | 3'469'200 |      17'420 |                     5.5 |

## Planned features
* Indexing of addresses / contracts

## To-do
* Friendly help/doc inside the script
* Include some unit-testing
* Better architecture to add other databases easily
* Document some benchmarks

