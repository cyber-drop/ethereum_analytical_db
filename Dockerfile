FROM ubuntu:16.04
# FROM python:3.5.2
# FROM docker.elastic.co/elasticsearch/elasticsearch:5.5.1

RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections

RUN apt-get update && apt-get install --yes software-properties-common gnupg

RUN apt-get install -y libcurl4-openssl-dev cmake python3-pip libpython3-dev libssl-dev build-essential automake pkg-config libtool libffi-dev libgmp-dev libyaml-cpp-dev

RUN apt-get install -y git

RUN apt-get install -y wget && apt-get install -y curl

RUN add-apt-repository ppa:webupd8team/java && apt-get update && apt-get install -y oracle-java8-installer

RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.5.1.deb

RUN dpkg -i elasticsearch-5.5.1.deb

WORKDIR /usr/src/core

COPY . .

RUN git clone https://github.com/ethereum/pyethereum && \
    cd ./pyethereum && \
    git checkout develop && \
    git checkout 3d5ec14032cc471f4dcfc7cc5c947294daf85fe0 && \
    pip3 install . && \
    cd ../

#RUN git clone https://github.com/Great-Hill-Corporation/quickBlocks && \
#    cd ./quickBlocks/src && \
#    git checkout master && \
#    git checkout 05f305ac3ce8eea27a21b52606588527f0131640 && \
#    cmake . && \
#    make && \
#    make install && \
#    cd ../../

RUN pip3 install -r ./requirements.txt

RUN curl http://localhost:9200

RUN nosetests .

CMD /bin/bash -c "python3 ./extractor.py --operation run-loop"