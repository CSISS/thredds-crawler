import sys
assert sys.version_info >= (3,5)

import datetime
import json


from lib.harvester import Harvester
from lib.collection_granule_indexer import CollectionGranuleIndexer

from lib.siphon.catalog import TDSCatalog, Dataset

from lib.indexdb import IndexDB
from lib.timestamp_util import timestamp_parser



class GranulesIndex():
    def __init__(self, base_dir):
        self.db = IndexDB("sqlite:///" + base_dir + "/thredds_granule_index.db", False)

        # self.db.drop_sql_tables()
        self.db.create_sql_tables()


    def is_index_expired(self, url):
        margin = datetime.timedelta(minutes = 10)
        cutoff = datetime.datetime.now() - margin

        index_datetime = self.db.get_collection_updated_at(url)
        if index_datetime and index_datetime > cutoff:
            print("index NOT EXPIRED")
            return False
        
        print("index EXPIRED")
        return True


    def index(self, collection_name, collection_xml_url):
        print("INDEX: %s %s" % (collection_xml_url, collection_name))

        
        if not self.is_index_expired(collection_xml_url):
            print("Recent index available for %s. Doing nothing" %  collection_xml_url)
            return

        indexer = CollectionGranuleIndexer()
        harvester = Harvester(indexer, 40, 10)

        harvester.harvest(collection_xml_url)
        results = indexer.indexes
        
        self.db.index_collection_granules(collection_name, collection_xml_url, results)

        return("discovered %d granules at %s" % (len(results), collection_xml_url))


    def get(self, collection_xml_url, start_time, end_time):
        start_time = timestamp_parser.parse_datetime(start_time)
        end_time = timestamp_parser.parse_datetime(end_time)

        granules = self.db.get_collection_granules(collection_xml_url, start_time, end_time)

        for g in granules:
            g['time_start'] = timestamp_parser.to_str(g['time_start'])
            g['time_end'] = timestamp_parser.to_str(g['time_end'])

        return(json.dumps(granules))


