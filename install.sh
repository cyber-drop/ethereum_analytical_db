sudo -H mkdir /usr/local/share/man

sudo apt-get install libcurl4-openssl-dev cmake python3-pip libpython3-dev libssl-dev build-essential automake pkg-config libtool libffi-dev libgmp-dev libyaml-cpp-dev

sudo -H pip3 install -r ./requirements.txt

git clone https://github.com/ethereum/pyethereum
cd ./pyethereum
git checkout develop
git checkout 3d5ec14032cc471f4dcfc7cc5c947294daf85fe0
sudo -H pip3 install .
cd ../

git clone https://github.com/Great-Hill-Corporation/quickBlocks
cd ./quickBlocks/src
git checkout master
git checkout 05f305ac3ce8eea27a21b52606588527f0131640
cmake .
make
sudo -H make install
cd ../../

git clone https://github.com/cyberFund/ethdrain
cd ./ethdrain
sudo -H pip3 install -r ./requirements.txt
cd ../

echo "ssh -L 9200:localhost:9200 cyberdrop@195.201.105.114"
echo "nano ./config.py"
echo "telegram-send --configure"