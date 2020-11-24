FROM grafana/grafana:latest

ENV GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=vertamedia-clickhouse-datasource 

COPY ./configure_grafana.sh /

ADD ./datasources /datasources
ADD ./dashboards /dashboards

ENTRYPOINT ["/configure_grafana.sh"]
