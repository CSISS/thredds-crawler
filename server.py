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


from granules_index import GranulesIndex

from lib.pycsw_helper import PycswHelper
from lib.harvester import Harvester

from lib.granule_scraper import GranuleScraper
from lib.collection_scraper import CollectionScraper
from lib.timestamp_util import timestamp_range_generator

from lib.util import mkdir_p

# from logging import info, debug,error

import logging


class PurgeExpiredResource(Resource):
    def post(self): 
        expired_date = timestamp_range_generator(14).start
        # expired_date = datetime.datetime.today() - datetime.timedelta(minutes=1)
    

        PycswHelper().delete_records({'where': 'TO_TIMESTAMP(records.insert_date, \'YYYY-MM-DDTHH:MI:SS\') <  TIMESTAMP \'' + str(expired_date) + '\'', 'values': []})
        return("purged expired (older than 14 days) records")


class GranulesIndexResource(Resource):
    def post(self):
        # get params
        collection_name = request.form['collection_name']  
        catalog_url = request.form['catalog_url']

        result = collection_xml_url
        result = granules_index.index(collection_name, catalog_url)

        print(result)
        return result

    def get(self):
        catalog_url = request.args['catalog_url']
        start_time = request.args['start_time']
        end_time = request.args['end_time']

        print(catalog_url)
        print(start_time)
        print(end_time)
        result = granules_index.get(catalog_url, start_time, end_time)

        # result = ""
        return result


class HarvestResource(Resource):
    def post(self, harvest_type):
        job_id = str(uuid.uuid4())[:8] # short id for uniqueness
        
        catalog_url = request.form['catalog_url']

        if harvest_type == 'granules':
            output_dir = RECORDS_DIR + '/granules.' + job_id
            mkdir_p(output_dir)

            scraper = GranuleScraper(output_dir)
        else:
            output_dir = RECORDS_DIR + '/collections.' + job_id
            tmp_dir = output_dir + '.tmp'
            mkdir_p(output_dir)
            mkdir_p(tmp_dir)

            scraper = CollectionScraper(output_dir, tmp_dir)

        # begin harvest
        harvester = Harvester(scraper, 40, 1)
        harvester.harvest(catalog_url)
     
        # complete
        print("harvest complete. importing %s" % output_dir)

        # load recortds
        PycswHelper().load_records(output_dir)
        return("harvested and loaded " + output_dir)



###############################

if __name__ == '__main__':
    RECORDS_DIR = '../records'
else:
    RECORDS_DIR = '/records'


granules_index = GranulesIndex(RECORDS_DIR)

application = Flask(__name__)
api = Api(application)



api.add_resource(GranulesIndexResource, '/granules_index')
api.add_resource(HarvestResource, '/harvest/<string:harvest_type>')
api.add_resource(PurgeExpiredResource, '/purge_expired')

# harvest.delete_lock('granules')
# harvest.delete_lock('collections')

if __name__ == '__main__':
    print("starting application on port 8002")
    application.run(port='8002', debug=False)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    application.logger.handlers = gunicorn_logger.handlers
    application.logger.setLevel(gunicorn_logger.level)
     