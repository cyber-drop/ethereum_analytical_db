# Internal transactions parsing
## Usage
To run API server, execute command below:
```bash
$ export FLASK_APP=./api/server.py
$ python3 -m flask run
```

To run internal transactions parsing, you can use command 
```bash
$ python3 ./extractor.py --index ELASTICSEARCH_INDEX --operation CHOSEN_OPERATION
```
Operation type can be selected from list below:
- detect-contracts, detect-internal-contracts

Runs a process of contract addresses detection for saved transactions. All transactions to contracts will be highlighted with 'to_contract' flag, each contract address will be extracted to a 'contract' collection of a selected index
- extract-traces

Starts traces extraction. Each transaction highlighted with 'to_contract' flag will get a field 'trace' with a trace extracted from parity
- parse-inputs, parse-internal-inputs

Starts input parsing. Each transaction highlighted with 'to_contract' flag will get a field 'decoded_input' with name of method called in contract and arguments for it.

- search-methods

Downloads contract bytecode and check does it contain signatures of token standards-specific methods. The list of standards then stored in 'standards' field. It also saves contract bytecode in 'bytecode' field. 

- extract-token-external-txs, extract-token-internal-txs

Downloads list of tokens from Coinmarketcap API and tries to find contracts with corresponding names in ES and then saves matching contracts into separate index. After finishing this process finds all transactions that have 'to' field equal to token contract address and also saves these transaction to separate index.

### Synchronization process

```bash
$ python3 ./extractor.py --operation prepare-indices # Prepare and optimize elasticsearch indices, run only once
$ python3 ./extractor.py --operation prepare-blocks # Prepare blocks index in elasticsearch (stub to run without ethdrain)
$ python3 ./extractor.py --operation extract-traces
$ python3 ./extractor.py --operation detect-internal-contracts
$ python3 ./extractor.py --operation extract-contracts-abi
$ python3 ./extractor.py --operation search-methods
$ python3 ./extractor.py --operation parse-internal-inputs
$ python3 ./extractor.py --operation extract-token-internal-txs
```