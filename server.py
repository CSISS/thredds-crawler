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
from lib.scraper.collection import CollectionScraper
from lib.util.datetime import timestamp_range_generator

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
        collection_name = request.form.get('collection_name')
        catalog_url = request.form.get('catalog_url')
        job_id = str(uuid.uuid4())[:8] # short id for uniqueness

        output_dir = RECORDS_DIR + '/index.' + job_id

        scraper = CollectionScraper(output_dir)
        
        driver = ScraperDriver(scraper, 20, 1)
        driver.harvest(catalog_url=catalog_url)
     
        # complete
        print("indexing harvest complete", flush=True)
        print("importing %s" % output_dir, flush=True)
        print("")


        # result = granules_index.update_index(collection_name, catalog_url)

        # print(result, flush=True)
        return output_dir

    def get(self):
        catalog_url = request.args['catalog_url']
        start_time = request.args['start_time']
        end_time = request.args['end_time']

        print(catalog_url)
        print(start_time)
        print(end_time)
        result = granules_index.get_index(catalog_url, start_time, end_time)

        # result = ""
        print("got index for %s" % catalog_url, flush=True)
        return result


class HarvestResource(Resource):
    def post(self, harvest_type):
        job_id = str(uuid.uuid4())[:8] # short id for uniqueness
        
        catalog_url = request.form['catalog_url']
        dataset_name = request.form.get('dataset_name', None)

        if harvest_type == 'granules':
            output_dir = RECORDS_DIR + '/granules.' + job_id
            # mkdir_p(output_dir)

            scraper = GranuleScraper(output_dir)
        # else:
        #     output_dir = RECORDS_DIR + '/collections.' + job_id
        #     tmp_dir = output_dir + '.tmp'
        #     mkdir_p(output_dir)
        #     mkdir_p(tmp_dir)

            scraper = CollectionScraper(output_dir, tmp_dir)

        # begin harvest
        harvester = ScraperDriver(scraper, 1, 1)
        harvester.harvest(catalog_url=catalog_url, dataset_name=dataset_name)
     
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
     