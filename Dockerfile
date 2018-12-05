FROM python:3.6-alpine

COPY . /opt/thredds-harvester-indexer

RUN apk add --no-cache gcc g++ libxslt-dev
RUN pip install --requirement /opt/thredds-harvester-indexer/requirements.txt


RUN adduser -D -u 1000 harvester
RUN mkdir /records
RUN chown -R harvester:harvester /records
VOLUME /records

USER harvester
WORKDIR /opt/thredds-harvester-indexer
EXPOSE 8000

CMD ["/bin/ash", "run_in_container.sh"]
