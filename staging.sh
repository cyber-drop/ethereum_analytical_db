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
    python3 ./ethdrain.py -s 5000000 -e 5050000 -o elasticsearch
    cd ../
    telegram-send "Transactions extracted"

    python3 ./extractor.py --operation detect-contracts
    telegram-send "Contracts detected"
    python3 ./extractor.py --operation extract-traces
    telegram-send "Traces extracted"

    telegram-send "Everything is done"
) 2>&1 | telegram-send --stdin
