# /granules_index
# collection_url=URL
# collection_name=NAME

import sys
assert sys.version_info >= (3,5)

import time
import threading

import os

import datetime
import time


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

from lib.siphon.catalog import TDSCatalog, Dataset, CatalogRef


# from logging import info, debug,error

import logging


# class PurgeExpiredResource(Resource):
#     def post(self): 
#         expired_date = timestamp_range_generator(14).start
#         # expired_date = datetime.datetime.today() - datetime.timedelta(minutes=1)
    

#         PycswHelper().delete_records({'where': 'TO_TIMESTAMP(records.insert_date, \'YYYY-MM-DDTHH:MI:SS\') <  TIMESTAMP \'' + str(expired_date) + '\'', 'values': []})
#         return("purged expired (older than 14 days) records")


class IndexResource(Resource):
    def post(self):
        print("1 TRAP SIGURG on " +  str(os.getpid()))
        debug_thread = threading.Thread(target = debug_dump_watch)
        debug_thread.start()



        # get params
        # collection_name = request.form.get('collection_name')

        catalog_url = request.form.get('catalog_url')

        if 'thredds.ucar.edu' in catalog_url:
            Dataset.default_authority = 'edu.ucar.unidata'

        if 'rda.ucar.edu' in catalog_url:
            Dataset.default_authority = 'edu.ucar.rda'

        job_id = str(uuid.uuid4())[:8] # short id for uniqueness

        index = Index(INDEX_DB_URL)

        output_dir = RECORDS_DIR + '/result.' + job_id

        scraper = CollectionImportScraper(output_dir, index)
        scraper.add_catalog(catalog_url=catalog_url)
        
        driver = ScraperDriver(scraper, 40, 1)
        driver.harvest()

        scraper.sync_index()
     
        # complete
        print("indexing harvest complete", flush=True)
        print("importing %s" % output_dir, flush=True)
        print("")

        PycswHelper().load_records(output_dir)

        return output_dir

    def get(self):
        index = Index(INDEX_DB_URL)

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
    def post(self):
        job_id = str(uuid.uuid4())[:8] # short id for uniqueness
        
        catalog_url = request.form['catalog_url']
        dataset_name = request.form.get('dataset_name', None)

        output_dir = RECORDS_DIR + '/result.' + job_id
        scraper = SimpleScraper(output_dir)
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
    # RECORDS_DIR = '../records'



# RECORDS_DIR = '../records'



sqlite_index_url = "sqlite:///" + RECORDS_DIR + '/index.sqlite.db'

INDEX_DB_URL = os.getenv('INDEX_DB_URL', sqlite_index_url)

# granules_index = GranulesIndex(RECORDS_DIR)

application = Flask(__name__)
api = Api(application)



api.add_resource(IndexResource, '/index')
api.add_resource(HarvestResource, '/harvest')

# harvest.delete_lock('granules')
# harvest.delete_lock('collections')

## DEBUG HANDLER
import sys, traceback, signal, threading, datetime, time

mutex = threading.Lock()

def debug_dump_watch():
    while True:
        print("Watching for /var/tmp/td", flush=True)
        time.sleep(1)
        try:
            if os.path.isfile('/var/tmp/td'):
                os.remove('/var/tmp/td')

                with mutex:
                    fname = '/var/tmp/trace-dump-' + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + '--' + str(os.getpid())
                    print("DEBUG SIGNAL " + fname)
                    
                    for thread_id, frame in sys._current_frames().items():
                        with open(fname, 'a+') as file:
                            file.write('Stack for thread {}\n'.format(thread_id))
                            traceback.print_stack(frame, file=file)
                            file.write('\n\n')
        except:
            e = sys.exc_info()[0]
            print(e)



if __name__ == '__main__':
    print("starting application on port 8002")
    application.run(port='8002', debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    application.logger.handlers = gunicorn_logger.handlers
    application.logger.setLevel(gunicorn_logger.level)
     