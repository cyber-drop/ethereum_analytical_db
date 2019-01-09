FROM docker.elastic.co/elasticsearch/elasticsearch:5.5.1

ADD ./elasticsearch.yml /usr/share/elasticsearch/config/

RUN /usr/share/elasticsearch/bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.5.1.0/elasticsearch-sql-5.5.1.0.zip