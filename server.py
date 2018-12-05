# /granules_index
# collection_url=URL
# collection_name=NAME

import sys
assert sys.version_info >= (3,6)

import time
import threading


from flask import Flask, request
from flask_restful import Resource, Api


from lib.threaded_harvester import ThreadedHarvester
from lib.collection_scraper import CollectionScraper
from lib.granule_scraper import GranuleScraper

from lib.siphon.catalog import TDSCatalog, Dataset

from granules_index import GranulesIndex



def perform_harvest(item_type):

    refs = {}
    top_cat = TDSCatalog('http://thredds.ucar.edu/thredds/catalog.xml')

    # GRANULES
    if item_type == 'granules':
        scraper = GranuleScraper(records_dir = RECORDS_DIR)

        refs['forecast'] = top_cat.catalog_refs['Forecast Model Data']
        # refs['satellite infrared'] = top_cat.catalog_refs['Satellite Data'].follow().catalog_refs['Infrared (11 um)']

        # refs['nexrad2-tjua'] = top_cat.catalog_refs['Radar Data'].follow() \
        #             .catalog_refs['NEXRAD Level II Radar WSR-88D'].follow() \
        #             .catalog_refs['TJUA']

        refs['nexrad3-pta-yux'] = top_cat.catalog_refs['Radar Data'].follow() \
                    .catalog_refs['NEXRAD Level III Radar'].follow()\
                    .catalog_refs['PTA'].follow() \
                    .catalog_refs['YUX']


    # COLLECTIONS
    else:
        scraper = CollectionScraper(records_dir = RECORDS_DIR)

        # refs['forecast'] = top_cat.catalog_refs['Forecast Model Data']
        refs['satellite infrared'] = top_cat.catalog_refs['Satellite Data'].follow().catalog_refs['Infrared (11 um)']

        refs['nexrad2-tjua'] = top_cat.catalog_refs['Radar Data'].follow() \
                    .catalog_refs['NEXRAD Level II Radar WSR-88D']


    harvester = ThreadedHarvester(scraper, 40, 10)

    harvester.harvest(refs.values())



class GranulesIndexResource(Resource):
    def post(self):
        # get params
        collection_name = request.form['collection_name']  
        collection_xml_url = request.form['collection_xml_url']

        result = collection_xml_url
        result = granules_index.index(collection_name, collection_xml_url)

        print(result)
        return result

    def get(self):
        collection_xml_url = request.args['collection_xml_url']
        start_time = request.args['start_time']
        end_time = request.args['end_time']

        print(collection_xml_url)
        print(start_time)
        print(end_time)
        result = granules_index.get(collection_xml_url, start_time, end_time)

        # result = ""
        return result

class HarvestResource(Resource):
    def post(self, item_type):
        t = threading.Thread(target=perform_harvest,args=(item_type,))
        t.start()
        return("started harvesting " + item_type + "...")

###############################

if __name__ == '__main__':
    RECORDS_DIR = 'records'
else:
    RECORDS_DIR = '/records'


granules_index = GranulesIndex(RECORDS_DIR)

application = Flask(__name__)
api = Api(application)

api.add_resource(GranulesIndexResource, '/granules_index') # Route_1
api.add_resource(HarvestResource, '/harvest/<string:item_type>') # Route_1

if __name__ == '__main__':
    application.run(port='8001', debug=True)
     