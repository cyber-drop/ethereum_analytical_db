# cyber•Drop core

## Installation

### With docker-compose

To build all nessesary containers (clickhouse, parity, tabix, core), use command:
```bash
docker-compose up
```

Check the correctness of the installation using
```bash
docker-compose run core test
```

Maybe, you'll have to wait a bit while parity will get an actual info from Ethereum chain

### With vanilla docker

To build docker container, use command

```bash
docker build -t cyberdrop/core .
```

To install parity, use:

```bash
docker pull parity/parity:stable
docker run -p 8545:8545 parity/parity --jsonrpc-interface=all --tracing=on
```

To install clickhouse, use:

```bash
docker pull yandex/clickhouse-server:18.12.17
docker run yandex/clickhouse-server -p 9000:9000 -p 8123:8123 
```

You can see actual options for these containers in docker-compose.yml file

Make sure you've activated clickhouse and parity ports. 

```bash
$ curl localhost:8545
Used HTTP Method is not allowed. POST or OPTIONS is required

$ curl localhost:9000
Port 9000 is for clickhouse-client program.
You must use port 8123 for HTTP.
```

Check the correctness of the installation using

```bash
docker run --network host cyberdrop/core test
```

### Configuration

Configuration is located in config.py file. Please check this list before installation:

```python
...

# URLs of parity APIs.
# You can specify block range for each URL to use different nodes for each request
PARITY_HOSTS = [...]

# Dictionary of table names in database.
# Meaning of each table explained in Schema
INDICES = {...}

# List of contract addresses to process in several operations.
# All other contracts will be skipped during certain operations
PROCESSED_CONTRACTS = [...]

# Size of pages received from Clickhouse
NUMBER_OF_JOBS = 1000 # recommended

# Number of chunks processed simultaneously during input parsing
INPUT_PARSING_PROCESSES = 10 # recommended

# Number of blocks processed simultaneously during events extraction
EVENTS_RANGE_SIZE = 10 # recommended

# API key for etherscan.io ABI extraction
ETHERSCAN_API_KEY = "..."

...
```

- INDICES - 
- PARITY_HOSTS - 
- NUMBER_OF_JOBS - 
- EVENTS_RANGE_SIZE - 
- INPUT_PARSING_PROCESSES - 
- PROCESSED_CONTRACTS - 
- ETHERSCAN_API_KEY - 
- ETHEREUM_START_DATE - 

## Usage

### Real-time synchronization

To start real-time synchronization loop, use:
```bash
# With vanilla docker
docker run --network host cyberdrop/core start

# With docker-compose
docker-compose run core start
```

To start synchronization with additional info for contracts whitelisted in config.py (extract ABI, parse inputs), use:
```bash
# With vanilla docker
docker run --network host cyberdrop/core start-full

# With docker-compose
docker-compose run core start-full
```

### Stats

Docker bundle contains tabix dashboard named "ETH SQL" that shows status of synchronization. You can look at the state of database [here](http://localhost:8080).

![Tabix Dashboard](./images/tabix.png)

This query checks the actual state over all blocks, unsynchronized blocks and contracts.

### Examples

Usage examples of the crawlers are located in **examples** dir of this repo. The actual list of examples goes below:
- [Gas price estimator](https://gitlab.com/cyberdrop/core/blob/docker_compose/examples/gas_price_estimation)

### Schema

Current data schema is going below:

```mermaid
graph LR
Block[ethereum-block <hr> <b>id #number</b> <br> number: integer <br> timestamp: timestamp]

BlockTracesExtracted[ ethereum-block-traces-extracted <hr> <b>id #number</b> <br> number: integer ]

Transaction[ethereum-internal-transaction <hr> <b>id #hash + position in trace</b> <br> blockNumber: integer <br> hash: string <br> from: string <br> to: string <br> value: float <br> input: string <br> output: string <br> gas: string <br> gasUsed: string <br> blockHash:string <br> transactionHash:string <br> transactionPosition:integer <br> subtraces: integer <br>traceAddress: array <br> type: string <br> callType:string <br> address:string <br> code:text <br> init: text <br> refundAddress:string <br>error: text <br>parent_error: boolean <br> balance: string <br> author: string <br> rewardType: string]

TransactionInput[ethereum-transaction-input <hr> <b> id#transaction id </b> <br> decoded_input: object]

TransactionFee[ethereum-transaction-fee <hr> <b> id#transaction id </b> <br> gasUsed: integer <br> gasPrice: float]

Contract[ethereum-contract <hr><b> id #transaction hash</b> <br>  address: string <br> blockNumber: integer <br> bytecode: text <br> creator: string <br> standards: array]

ContractABI[ethereum-contract-abi <hr> <b>id#contract</b> <br> contract: string <br> abi: string <br> abi_extracted: boolean]

ERC20Token[ethereum-contract-token-erc20 <hr><b>id#contract</b><br> decimals: integer <br> contract: string <br> token_name: string <br> token_owner: string <br> total_supply: string <br> token_symbol: string <br> cc_sym: string <br> cmc_id: integer]

ERC721Token[ethereum-contract-token-erc721 <hr><b>id#contract</b><br> contract: string <br> token_name: string <br> token_owner: string <br> token_symbol: string]

ERC20TokenTransaction[ethereum-token-transaction-erc20 <hr> <b>id#tx_hash</b> <br> tx_hash: string <br> block_id: integer <br> token: string <br> valid: boolean <br> value: float <br> to: string <br> from: string <br> method: string]

Price[ethereum-token-price <hr> <b>id#token_symbol + _  + date </b><br> token: string <br> BTC: float <br> USD: float <br> ETH: float <br> USD_cmc: float <br> marketCap: integer <br> timestamp: timestamp]

ERC721TokenTransaction[ethereum-token-transaction-erc721 <hr> <b>id#tx_hash</b> <br> tx_hash: string <br> block_id: integer <br> token: string <br> valid: boolean <br> token_id: string <br> to: string <br> from: string <br> method: string]

BlockTracesExtracted --> |number| Block
Transaction -->|blockNumber| Block
TransactionInput -->|id|Transaction
TransactionFee -->|id|Transaction
Contract -->|blockNumber| Block
Contract --> |parent_transaction| Transaction
ContractABI -->|contract| Contract
ERC20Token -->|contract| Contract
ERC721Token -->|contract| Contract
ERC20TokenTransaction --> |token| ERC20Token
ERC721TokenTransaction --> |token| ERC721Token
ERC20TokenTransaction -->|block_id| Block
ERC721TokenTransaction -->|block_id| Block
Price --> |token:cc_sym|ERC20Token

style Contract fill:#fff;
style ERC20TokenTransaction fill:#fff;
style ERC721TokenTransaction fill:#fff;

style TransactionFee stroke-dasharray:3,3;
style Block stroke-dasharray:3,3;

style Block stroke-width:5px;
style Transaction stroke-width:5px;
style BlockTracesExtracted stroke-width:5px;
style Contract stroke-width:5px;
style ContractABI stroke-width:5px;
style TransactionFee stroke-width:5px;

subgraph ERC-721 standard
ERC721Token
ERC721TokenTransaction
end
```

### Architecture

All components of this repo and their interactions can be found below:

```
TODO Will be updated
```

![Architecture](./images/core.png)

### Operations
```bash
$ docker-compose run core --help

Usage: extractor.py [OPTIONS] COMMAND [ARGS]...

  Ethereum extractor

Options:
  --help  Show this message and exit.

Commands:
  prepare-database               Prepare all indices and views in database
  start                          Run partial synchronization of the database.
  start-full                     Run full synchronization of the database
  
  prepare-contracts-view         Prepare material view with contracts
  prepare-erc-transactions-view  Prepare material view with erc20
                                 transactions
  prepare-indices                Prepare tables in database
  extract-blocks                 Extract blocks with timestamp
  extract-events                 Extract events
  extract-traces                 Extract internal transactions
  extract-tokens                 Extract ERC20 token names, symbols, 
                                 total supply and etc.
  download-contracts-abi         Extract ABI description from etherscan.io
  download-prices                Download exchange rates
  parse-events-inputs            Start input parsing for events.
  parse-transactions-inputs      Start input parsing for transactions.
  
  test                           Run tests
```
