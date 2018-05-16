sudo apt-get install libcurl4-openssl-dev cmake python3-pip libpython3-dev libssl-dev build-essential automake pkg-config libtool libffi-dev libgmp-dev libyaml-cpp-dev

sudo -H pip3 install -r ./requirements.txt

git clone https://github.com/ethereum/pyethereum
cd ./pyethereum
sudo -H pip3 install .
cd ../

git clone https://github.com/Great-Hill-Corporation/quickBlocks
cd ./quickBlocks/src
cmake .
make
sudo -H make install
cd ../../

echo "ssh -L 9200:localhost:9200 cyberdrop@195.201.105.114"
echo "nano ./config.py"