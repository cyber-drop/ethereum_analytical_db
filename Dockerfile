FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noniteractive
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV TZ=Europe/London

RUN apt-get update && \
    apt-get install -y tzdata libcurl4-openssl-dev cmake libssl-dev build-essential automake pkg-config libtool libffi-dev libgmp-dev libyaml-cpp-dev && \
    apt-get install -y git && \
    apt-get install -y wget && apt-get install -y curl && \
    apt-get install -y software-properties-common

RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y libpython3.8-dev python3.8-distutils

RUN curl https://bootstrap.pypa.io/get-pip.py | python3.8

RUN git clone https://github.com/ethereum/pyethereum && \
    cd ./pyethereum && \
    git checkout develop && \
    git checkout 3d5ec14032cc471f4dcfc7cc5c947294daf85fe0 && \
    pip3.8 install --default-timeout=100 . && \
    cd ../

WORKDIR /usr/src/core

ADD ./requirements.txt .

RUN pip3.8 install --default-timeout=100 -r ./requirements.txt

ADD . .

ENTRYPOINT ["python3.8", "./extractor.py"]
