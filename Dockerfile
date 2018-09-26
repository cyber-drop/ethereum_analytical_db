FROM ubuntu:16.04

RUN apt-get update && \
    apt-get install -y libcurl4-openssl-dev cmake libssl-dev build-essential automake pkg-config libtool libffi-dev libgmp-dev libyaml-cpp-dev && \
    apt-get install -y git && \
    apt-get install -y wget && apt-get install -y curl && \
    apt-get install -y software-properties-common

RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.6 libpython3.6-dev

RUN curl https://bootstrap.pypa.io/get-pip.py | python3.6

WORKDIR /usr/src/core

ADD . .

#RUN git clone https://github.com/Great-Hill-Corporation/quickBlocks && \
#    cd ./quickBlocks/src && \
#    git checkout master && \
#    git checkout 05f305ac3ce8eea27a21b52606588527f0131640 && \
#    cmake . && \
#    make && \
#    make install && \
#    cd ../../

#RUN git clone https://github.com/ethereum/pyethereum && \
#    cd ./pyethereum && \
#    git checkout develop && \
#    git checkout 3d5ec14032cc471f4dcfc7cc5c947294daf85fe0 && \
#    pip3.6 install --default-timeout=100 . && \
#    cd ../

RUN pip3.6 install --default-timeout=100 -r ./requirements.txt

#RUN nosetests .

CMD /bin/bash -c "sleep 10 && python3.6 ./extractor.py --operation prepare-indices && python3.6 ./extractor.py --operation run-loop"
