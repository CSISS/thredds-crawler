FROM csiss/pycsw

USER root

COPY ./requirements.txt /tmp/thredds-crawler-requirements.txt

# RUN apk add --no-cache gcc g++ libxslt-dev
RUN pip3 install --requirement /tmp/thredds-crawler-requirements.txt

COPY . /opt/thredds-crawler

# RUN adduser -D -u 1000 harvester
RUN mkdir /records
RUN chown -R pycsw:pycsw /records
VOLUME /records

USER pycsw
WORKDIR /opt/thredds-crawler
EXPOSE 8000

ENTRYPOINT ["/bin/ash", "entrypoint.sh"]
