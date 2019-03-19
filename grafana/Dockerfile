FROM grafana/grafana:4.6.4

ENV GF_INSTALL_PLUGINS="vertamedia-clickhouse-datasource"

COPY ./configure_grafana.sh /

ADD ./datasources /datasources
ADD ./dashboards /dashboards

ENTRYPOINT ["/configure_grafana.sh"]