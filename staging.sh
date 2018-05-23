telegram-send "Staging started"
cd /home/$USER/core
pkill -9 extractor.py
pkill -9 ethdrain.py

curl -X DELETE localhost:9200/ethereum-transaction
curl -X DELETE localhost:9200/ethereum-internal-transaction
curl -X DELETE localhost:9200/ethereum-block
curl -X DELETE localhost:9200/ethereum-contract

(
    python3 ./extractor.py --operation prepare-indices
    telegram-send "Indices prepared"

    cd ./ethdrain
    python3 ./ethdrain.py -s 5000000 -e 5005000 -o elasticsearch -r http://localhost:8550
    cd ../
    telegram-send "Transactions extracted"

    python3 ./extractor.py --operation detect-contracts
    telegram-send "Contracts detected"
    python3 ./extractor.py --operation extract-traces
    telegram-send "Traces extracted"
    python3 ./extractor.py --operation detect-internal-contracts
    telegram-send "Internal contracts detected"
    python3 ./extractor.py --operation extract-traces
    telegram-send "Traces re-extracted"
    python3 ./extractor.py --operation search-methods
    telegram-send "Contracts info added"
    python3 ./extractor.py --operation parse-inputs
    telegram-send "Inputs parsed"
    python3 ./extractor.py --operation parse-internal-inputs
    telegram-send "Internal inputs parsed"

    telegram-send "Everything is done"
) 2>&1 | telegram-send --stdin
