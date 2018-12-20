# /granules_index
# collection_url=URL
# collection_name=NAME

import sys
assert sys.version_info >= (3,5)

import time
import threading

import os

from flask import Flask, request
from flask_restful import Resource, Api

import uuid


from granules_index import GranulesIndex
import harvest


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
        
        records_dir = harvest.do_harvest(catalog_url, harvest_type, job_id)

        return(records_dir)



        # harvest
        # t = threading.Thread(target=harvest.do_harvest,args=(item_type,))
        # t.start()
        # return("started harvesting " + item_type + "...")

    # def get(self, item_type):
    #     if(harvest.exists_lock(item_type)):
    #         return("harvesting")
    #     else:
    #         return("done")


###############################

if __name__ == '__main__':
    RECORDS_DIR = '../records'
else:
    RECORDS_DIR = '/records'


granules_index = GranulesIndex(RECORDS_DIR)

application = Flask(__name__)
api = Api(application)



api.add_resource(GranulesIndexResource, '/granules_index') # Route_1
api.add_resource(HarvestResource, '/harvest/<string:harvest_type>') # Route_1

# harvest.delete_lock('granules')
# harvest.delete_lock('collections')

if __name__ == '__main__':
    print("starting application on port 8002")
    application.run(port='8002', debug=True)
     