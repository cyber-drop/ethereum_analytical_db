FROM ubuntu:16.04

RUN apt-get update && \
    apt-get install -y libcurl4-openssl-dev cmake python3-pip libpython3-dev libssl-dev build-essential automake pkg-config libtool libffi-dev libgmp-dev libyaml-cpp-dev && \
    apt-get install -y git && \
    apt-get install -y wget && apt-get install -y curl

WORKDIR /usr/src/core

ADD . /core

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.3.0/wait /wait

RUN git clone https://github.com/ethereum/pyethereum && \
    cd ./pyethereum && \
    git checkout develop && \
    git checkout 3d5ec14032cc471f4dcfc7cc5c947294daf85fe0 && \
    pip3 install . && \
    cd ../

RUN git clone https://github.com/Great-Hill-Corporation/quickBlocks && \
    cd ./quickBlocks/src && \
    git checkout master && \
    git checkout 05f305ac3ce8eea27a21b52606588527f0131640 && \
    cmake . && \
    make && \
    make install && \
    cd ../../

RUN pip3 install -r ./requirements.txt

RUN chmod +x /wait

RUN nosetests .

CMD /bin/bash -c "/wait && python3 ./extractor.py --operation run-loop"