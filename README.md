# Internal transactions parsing
## Usage
To run internal transactions parsing, you can use command 
```bash
$ python3 ./extractor.py --index ELASTICSEARCH_INDEX --operation CHOSEN_OPERATION
```
Operation type can be selected from list below:
- detect-contracts

Runs a process of contract addresses detection for saved transactions. All transactions to contracts will be highlighted with 'to_contract' flag, each contract address will be extracted to a 'contract' collection of a selected index
- extract-traces

Starts traces extraction. Each transaction highlighted with 'to_contract' flag will get a field 'trace' with a trace extracted from parity
- parse-inputs

Starts input parsing. Each transaction highlighted with 'to_contract' flag will get a field 'decoded_input' with name of method called in contract and arguments for it.

## Operations speed

| Operation                                  | Maximum allowed batch size | Speed               | Starts from stop point |
|--------------------------------------------|----------------------------|---------------------|------------------------|
| detect-contracts (Find contract addresses) | 10000                      | 1000 transactions/s | No                     |
| detect-contracts (Set to_contract flag)    | 1000                       | 16 contracts/s      | Yes                    |
| extract-traces                             | 1000                       | 75 transactions/s   | Yes                    |
| parse-inputs                               | 1000                       | 3 transactions/s    | No                     |

## Current status
Current status of parsing can be seen on kibana dashboard:
http://localhost:5601/app/kibana#/dashboard/4e400ef0-47af-11e8-a68a-cdcd3cdf86f2?embed=true&_g=()

Don't forget to forward 5061 port from mercury.cyber.fund:
```bash
ssh -p 33325 -L 5601:localhost:5601 cyberanalytics@mercury.cyber.fund
```