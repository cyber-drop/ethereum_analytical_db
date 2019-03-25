# Balances API

Simple API to show user balance and token balance

## Installation

```
pip3 install --user -r ./requirements.txt
```

## Usage

Run API server
```
python3 ./server.py
```

Make some requests:

```bash
# Get ethereum balances
$ curl -XPOST -d '["0x89051889d9219ecdc6de483f30d7fa31a89e8b0d"]' localhost:5000/balances/
{"0x89051889d9219ecdc6de483f30d7fa31a89e8b0d": 0.4725110100000034}

# Get token balances
$ curl -XPOST -d '["0xf8ea34b1c2f1cd024da35a3fef73e133d116793e"]' localhost:5000/token_balances/0x00c6659f0aae093910a5609cfaaabac5c0b57995
{"0xf8ea34b1c2f1cd024da35a3fef73e133d116793e": 4937960.494488163}
```