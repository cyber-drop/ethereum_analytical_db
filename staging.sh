echo "Staging started"
cd /home/$USER/core

curl -X DELETE localhost:9200/ethereum-transaction?pretty
curl -X DELETE localhost:9200/ethereum-internal-transaction?pretty
curl -X DELETE localhost:9200/ethereum-block?pretty
curl -X DELETE localhost:9200/ethereum-contract?pretty
curl -X DELETE localhost:9200/ethereum-listed-token?pretty
curl -X DELETE localhost:9200/ethereum-token-transaction?pretty

run_staging ()
{
    python3 ./extractor.py --operation detect-contracts
    echo "Contracts detected"
    python3 ./extractor.py --operation extract-traces
    echo "Traces extracted"
    python3 ./extractor.py --operation detect-internal-contracts
    echo "Internal contracts detected"
    python3 ./extractor.py --operation extract-contracts-abi
    echo "Contracts ABI extracted"
    python3 ./extractor.py --operation search-methods
    echo "Contracts info added"
    python3 ./extractor.py --operation parse-inputs
    echo "Inputs parsed"
    python3 ./extractor.py --operation parse-internal-inputs
    echo "Internal inputs parsed"
    python3 ./extractor.py --operation extract-tokens-txs
    echo "Tokens transactions extracted"
}

python3 ./extractor.py --operation prepare-indices
echo "Indices prepared"

cd ./ethdrain
python3 ./ethdrain.py -s 5010000 -e 5020000 -o elasticsearch -r http://localhost:8545
cd ../
echo "Transactions extracted"

run_staging

cd ./ethdrain
python3 ./ethdrain.py -s 5020000 -e 5030000 -o elasticsearch -r http://localhost:8545
cd ../
echo "Transactions extracted"

run_staging

telegram-send "Staging is completed, see tmux a -t staging"
telegram-send "Spoiler: everything is on fire"