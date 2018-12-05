#!/bin/sh
mkdir -p /records/collections
mkdir -p /records/granules
mkdir -p /records/scraped

/usr/local/bin/gunicorn --workers 10 --bind 0.0.0.0:8000 server