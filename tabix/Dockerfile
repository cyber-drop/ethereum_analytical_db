FROM spoonest/clickhouse-tabix-web-client

SHELL ["/bin/bash", "-c"]

RUN mkdir -p /var/www/html/custom-dashboards/

COPY ./dashboards.js /var/www/html/custom-dashboards/

RUN INDEX=$(cat /var/www/html/index.html) && \
    SCRIPT="<script src='/custom-dashboards/dashboards.js'></script>" && \
    INDEX=${INDEX/<body>/<body>$SCRIPT} && \
    echo $INDEX > /var/www/html/index.html


