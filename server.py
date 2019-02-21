# /granules_index
# collection_url=URL
# collection_name=NAME

import sys
assert sys.version_info >= (3,5)

import time
import threading

import os

import datetime


from flask import Flask, request
from flask_restful import Resource, Api

import uuid


from lib.index import Index

from lib.pycsw_helper import PycswHelper
from lib.scraper_driver import ScraperDriver

from lib.scraper.simple import SimpleScraper

from lib.scraper.collection_import import CollectionImportScraper
from lib.scraper.collection_refresh import CollectionRefreshScraper

from lib.util.dtutil import timestamp_range_generator, timestamp_parser

from lib.util.path import mkdir_p

# from logging import info, debug,error

import logging


class PurgeExpiredResource(Resource):
    def post(self): 
        expired_date = timestamp_range_generator(14).start
        # expired_date = datetime.datetime.today() - datetime.timedelta(minutes=1)
    

        PycswHelper().delete_records({'where': 'TO_TIMESTAMP(records.insert_date, \'YYYY-MM-DDTHH:MI:SS\') <  TIMESTAMP \'' + str(expired_date) + '\'', 'values': []})
        return("purged expired (older than 14 days) records")


class IndexResource(Resource):
    def post(self):
        # get params
        # collection_name = request.form.get('collection_name')
        catalog_url = request.form.get('catalog_url')
        job_id = str(uuid.uuid4())[:8] # short id for uniqueness

        index = Index(INDEX_FILE)

        output_dir = RECORDS_DIR + '/result.' + job_id

        scraper = CollectionImportScraper(output_dir, index)
        scraper.add_catalog(catalog_url=catalog_url)
        
        driver = ScraperDriver(scraper, 20, 1)
        driver.harvest()

        scraper.sync_index()
     
        # complete
        print("indexing harvest complete", flush=True)
        print("importing %s" % output_dir, flush=True)
        print("")

        PycswHelper().load_records(output_dir)

        return output_dir

    def get(self):
        index = Index(INDEX_FILE)

        collection_name = request.args.get('collection_name')
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')

        start_time = timestamp_parser.parse_datetime(start_time, default=timestamp_parser.min_datetime)
        end_time = timestamp_parser.parse_datetime(end_time, default=timestamp_parser.max_datetime)

        # refresh if needed (might be time consuming)
        scraper = CollectionRefreshScraper(index)
        scraper.set_refresh_scope(collection_name, start_time, end_time)
        
        driver = ScraperDriver(scraper, 4, 0.5)
        driver.harvest()

        scraper.sync_index()

        # retrieve from index DB
        result = index.get_granules(collection_name, start_time, end_time)

        print("read index for %s" % collection_name, flush=True)
        return result


class HarvestResource(Resource):
    def post(self, harvest_type):
        job_id = str(uuid.uuid4())[:8] # short id for uniqueness
        
        catalog_url = request.form['catalog_url']
        dataset_name = request.form.get('dataset_name', None)

        output_dir = RECORDS_DIR + '/result.' + job_id
        scraper = GranuleScraper(output_dir)
        scraper.add_catalog(catalog_url=catalog_url, dataset_name=dataset_name)

        # begin harvest
        harvester = ScraperDriver(scraper, 20, 1)
        harvester.harvest()
     
        # complete
        print("harvest complete", flush=True)
        print("importing %s" % output_dir, flush=True)
        print("")

        # load records
        PycswHelper().load_records(output_dir)
        print("records loaded", flush=True)
        return("harvested and loaded " + output_dir)



###############################

if __name__ == '__main__':
    RECORDS_DIR = '../records'
else:
    RECORDS_DIR = '/records'

INDEX_FILE = RECORDS_DIR + '/index.sqlite.db'


# granules_index = GranulesIndex(RECORDS_DIR)

application = Flask(__name__)
api = Api(application)



api.add_resource(IndexResource, '/index')
api.add_resource(HarvestResource, '/harvest')
api.add_resource(PurgeExpiredResource, '/purge_expired')

# harvest.delete_lock('granules')
# harvest.delete_lock('collections')

if __name__ == '__main__':
    print("starting application on port 8002")
    application.run(port='8002', debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    application.logger.handlers = gunicorn_logger.handlers
    application.logger.setLevel(gunicorn_logger.level)
     