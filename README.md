# cyber•Drop core

## Installation

To build contents of this repo, use command:

```bash
./install.sh
```

Make sure you are done with everything correctly:
```bash
nosetests .
```

## Configuration

Configuration is located in config.py file. Please check this list before installation:
- INDICES - Dictionary of index names in ElasticSearch
- PARITY_HOSTS - URLs of parity APIs. You can specify block range for each URL to use different nodes for each request
- NUMBER_OF_JOBS - Size of pages received from ElasticSearch
- PROCESSED_CONTRACTS - List of contract addresses to process in several operations. All other contracts will be skipped

## Usage

### History synchronization process
To start synchronization process at first time without starting point dump, use:

```bash
python3 ./extractor.py --operation prepare-indices
python3 ./extractor.py --operation prepare-blocks
python3 ./extractor.py --operation extract-traces
python3 ./extractor.py --operation detect-contracts
python3 ./extractor.py --operation detect-contract-transactions
python3 ./extractor.py --operation extract-contracts-abi
python3 ./extractor.py --operation search-methods
python3 ./extractor.py --operation extract-prices
python3 ./extractor.py --operation parse-inputs
python3 ./extractor.py --operation extract-token-transactions
python3 ./extractor.py --operation extract-token-transactions-prices-usd
python3 ./extractor.py --operation extract-token-transactions-prices-eth
```

### Dump installation
To start from existed database dump, use:
```bash
TODO
```

### Real-time synchronization
To start real-time synchronization loop, use:

```bash
python3 ./extractor.py --operation run-loop
```

### Operations
Operation type can be selected from list below:

- prepare-indices

Prepare indices in ElasticSearch

- prepare-blocks (blocks.py)

Extract blocks with timestamps to ElasticSearch

```mermaid
graph TD
A[Begin] --> B[Get last block from parity]
B --> C[Get last block from database]
C --> D(For numbers chunk from last database block + 1 to last parity block)
D --> E(For number in chunk)
E --> F[Extract timestamp for block number from parity]
F --> E
E --> |no more numbers|G[Save extracted blocks]
G --> D
D --> |no more numbers|H[End]
```

- extract-traces (internal_transactions.py)

Starts extraction of internal ethereum transactions

- detect-contracts (contract_transactions.py)

Extract contract info (address, bytecode, author, etc) from transactions

```mermaid
graph TD
A[Begin] --> B(For transactions chunk with 'created' field)
B --> C(For transaction in chunk)
C --> D{Transaction contains 'error' field}
D --> |no|E[Extract contract with bytecode, owner, bytecode, block number, parent transaction]
E --> C
D --> |yes|C
C --> |no more transactions|F[Save extracted contracts to database]
F --> G[Save contract_created flag for transactions chunk]
G --> B
B --> |no more transactions|End
```

- detect-contract-transactions (contract_transactions.py)

Highlight all transactions to contracts with to_contract flag

```mermaid
graph TD
A[Begin] --> B[Get current max block from ElasticSearch]
B --> C(For contract chunk in contracts with itx_transactions_detected_block < current max block)
C --> D(For contract in chunk)
D --> E[Add contract address and itx_transactions_detected_block to request]
E --> D
D --> |no more contracts| F[Set to_contract flag to transactions by generated request before current max block]
F --> G[Save itx_transactions_detected_block = current max block for contracts]
G --> C
C --> |no more contracts|H[End]
```

- extract-contracts-abi (contracts.py)

Extract ABI description from etherscan.io for specified contracts

```mermaid
graph TD
A[Begin] 
A --> B(For contract chunk in contracts without abi_extracted flag)
B --> C(For contract in chunk)
C --> D{contract ABI in cache}
D --> |yes|F[Get ABI from cache]
D --> |no|E[Get ABI from etherscan.io]
E --> G[Save ABI to cache]
G --> H[Extract ABI]
F --> H
H --> C
C --> |no more contracts| I[Save extracted ABIs and abi_extracted flag for contracts]
I --> J[End]
```

- parse-inputs (contracts.py)

Starts input parsing. Each transaction will get a field 'decoded_input' with name of method and arguments description

```mermaid
graph TD
A[Begin] 
A --> B[Get current max block from ElasticSearch]
B --> C(For contract chunk in contracts with ABI and itx_inputs_decoded_block < current max block)
C --> D(For contract in chunk)
D --> E[Add contract address and itx_transactions_detected_block to request]
E --> D
D --> |no more contracts| F(For transactions chunk in transactions by generated request)
F --> G(For transaction in chunk)
G --> H[Extract name and params for transaction input according to ABI]
H --> G
G --> |no more transactions|I[Save extracted names and params to decoded_input field]
I --> F
F --> |no more transactions|J[Save itx_transactions_detected_block = current max block fro contracts]
J --> C
C --> |no more contracts|K[End]
```

- search-methods (contract_methods.py)

Checks if contracts contain signatures of standards-specific methods. The list of standards stored in 'standards' field.
It also saves ERC20 token names, symbols, total supply and etc.

```mermaid
graph TD
A[Begin] --> B(For contracts chunk in contracts without 'methods' flag)
B --> C(For contract in chunk)
C --> D{Bytecode contrains transfer signature}
D --> |yes|E[Find standards]
E --> F[Extract name, symbol and owner from parity]
F --> G{Can extract decimals}
G --> |yes|H[Extract decimals from parity]
G --> |no|I[Set decimals = 18]
I --> J{Can extract total_supply}
H --> J
J --> |yes|K[Extract total_supply from parity]
K --> K1[Convert total_suppy from wei to eth]
J --> |no|L[Set total_supply = 0]
L --> M[Update contract with extracted name, symbol, owner, decimals, total_supply, standards, is_token:true]
K1 --> M
D --> |no|O[Update contract with is_token:false]
O --> P[Update contract with methods:true]
M --> P
P --> Q[Save contract to database]
Q --> C
C --> |no more contracts| B
B --> |no more contracts| R[End]
```

- extract-token-transactions (token_holders.py)

Downloads list of tokens from coinmarketcap API and tries to find contracts with corresponding names in ES and then saves matching contracts into separate index. After finishing this process finds all transactions that have 'to' field equal to token contract address and also saves these transaction to separate index.

```mermaid
graph TD
A[Begin] 
A --> B[Get current max block from ElasticSearch]
B --> C(For contract chunk in contracts with cmc_id and itx_token_transactions_extracted_block < current max block)
C --> D(For contract in chunk)
D --> E[Extract total_supply transaction]
E --> D
D --> |no more contracts|F[Save extracted transactions to ElasticSearch]
F --> G(For contract in chunk)
G --> H[Add contract address and itx_token_transactions_extracted_block to request]
H --> G
G --> |no more contracts| I(For transactions chunk in transactions by generated request)
I --> J(For transaction in chunk)
J --> K{No error, decoded_input presented}
K --> |yes| L{Method can transfer tokens}
L --> M[Extract transaction]
K --> |no|J
L --> |no|J
M --> J
J --> |no more transactions|O[Save extracted transactions to ElasticSearch]
O --> C
C --> |no more contracts| P[End]
```

- extract-prices (token_prices.py)

Download token capitalization, ETH, BTC and USD prices from cryptocompare and coinmarketcap

- extract-token-transactions-prices-* (token_transactions_prices.py)

Set USD and ETH prices for transactions. Also set an "overflow" field - a probability that transaction value is corrupted,
i.e. is greater than market capitalization

- run-loop

Runs real-time synchronization loop

```mermaid
graph TD
A[Begin] --> B[Prepare blocks]
B --> C[Extract internal transactions]
C --> D[Extract contracts]
D --> E[Extract contract transactions]
E --> F[Extract contracts ABI]
F --> G[Parse transactions inputs]
G --> H[Extract token prices]
H --> I[Extract token transactions]
I --> J[Extract USD transaction values]
J --> K[Extract ETH transaction values]
K --> B
```

### Indices

Current data schema is going below:

```mermaid
graph LR
Block[ethereum-block <hr> <b>id #number</b> <br> number: integer <br> timestamp: timestamp]

Transaction[ethereum-internal-transaction <hr> <b>id #hash + position in trace</b> <br> blockNumber: integer <br> hash: string <br> from: string <br> to: string <br> value: float <br> input: string <br> output: string <br> gas: string <br> gasUsed: string <br> blockHash:string <br> transactionHash:string <br> transactionPosition:integer <br> subtraces: integer <br>traceAddress: array <br> type: string <br> callType:string <br> address:string <br> code:text <br> init: text <br> refundAddress:string <br>error: text <br>parent_error: boolean <br> balance: string]

MiningTransaction[ethereum-miner-transaction <hr> <b>id #blockHash</b> <br> blockNumber: integer<br> author: string <br> blockHash: string <br> rewardType: string <br> subtraces: integer <br> type: string <br> value: float]

Contract[ethereum-contract <hr><b> id #address</b> <br>  address: string <br> parent_transaction: string <br> blockNumber: integer <br> abi: object <br> bytecode: text <br> decimals: integer <br> owner: string <br> standards: array <br> token_name: string <br> token_owner: string <br> total_supply: string <br> token_symbol: string <br> cc_sym: string <br> cmc_id: integer]

TokenTransaction[ethereum-token-transaction <hr> <b>id#tx_hash</b> <br> tx_hash: string <br> block_id: integer <br> token: string <br> tx_hash: string <br> valid: boolean <br> raw_value: string <br> value: float <br> to: string <br> from: string <br> method: string]

Price[ethereum-token-price <hr> <b>id#token_symbol + _  + date </b><br> token: string <br> BTC: float <br> USD: float <br> ETH: float <br> USD_cmc: float <br> marketCap: integer <br> timestamp: timestamp]

Transaction -->|blockNumber| Block
MiningTransaction -->|blockNumber| Block
Contract -->|blockNumber| Block
Contract --> |parent_transaction| Transaction
TokenTransaction --> |token| Contract
TokenTransaction -->|block_id| Block
Price --> |token:cc_sym|Contract
```

### Architecture

All components of this repo and their interactions can be found below: