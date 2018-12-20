FROM csiss/pycsw

USER root

COPY ./requirements.txt /tmp/thredds-harvester-indexer-requirements.txt

# RUN apk add --no-cache gcc g++ libxslt-dev
RUN pip3 install --requirement /tmp/thredds-harvester-indexer-requirements.txt

COPY . /opt/thredds-harvester-indexer

# RUN adduser -D -u 1000 harvester
RUN mkdir /records
RUN chown -R pycsw:pycsw /records
VOLUME /records

USER pycsw
WORKDIR /opt/thredds-harvester-indexer
EXPOSE 8000

ENTRYPOINT ["/bin/ash", "run_in_container.sh"]
