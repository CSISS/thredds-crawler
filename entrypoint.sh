#!/bin/sh

# /usr/local/bin/gunicorn --workers 10 --bind 0.0.0.0:8000 server
/usr/bin/python3.5 /usr/bin/gunicorn --workers 6 --capture-output --log-level INFO --timeout 3600 --bind 0.0.0.0:8000 server