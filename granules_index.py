import sys
assert sys.version_info >= (3,6)

import datetime
import json


from lib.threaded_harvester import ThreadedHarvester
from lib.collection_granule_indexer import CollectionGranuleIndexer

from lib.siphon.catalog import TDSCatalog, Dataset

from lib.indexdb import IndexDB
from lib.config import config
from lib.timestamp_util import timestamp_parser



class GranulesIndex():
    def is_index_expired(db, url):
        margin = datetime.timedelta(minutes = 10)
        cutoff = datetime.datetime.now() - margin

        index_datetime = db.get_collection_updated_at(url)
        if index_datetime and index_datetime > cutoff:
            print("index NOT EXPIRED")
            return False
        
        print("index EXPIRED")
        return True


    def index(collection_name, collection_xml_url):
        print("INDEX: %s %s" % (collection_xml_url, collection_name))

        db = IndexDB(config['index_db_url'])

        if not GranulesIndex.is_index_expired(db, collection_xml_url):
            print("Recent index available for %s. Doing nothing" %  collection_xml_url)
            exit(0)

        indexer = CollectionGranuleIndexer()
        harvester = ThreadedHarvester(indexer, 40, 10)
        catalog = TDSCatalog(collection_xml_url)

        harvester.harvest(catalog.catalog_refs.values())
        results = indexer.indexes
        
        db.index_collection_granules(collection_name, collection_xml_url, results)

        return("discovered %d granules at %s" % (len(results), collection_xml_url))


    def get(collection_xml_url, start_time, end_time):
        start_time = timestamp_parser.parse_datetime(start_time)
        end_time = timestamp_parser.parse_datetime(end_time)

        db = IndexDB(config['index_db_url'])

        granules = db.get_collection_granules(collection_xml_url, start_time, end_time)

        for g in granules:
            g['time_start'] = timestamp_parser.to_str(g['time_start'])
            g['time_end'] = timestamp_parser.to_str(g['time_end'])

        return(json.dumps(granules))



# if __name__ == '__main__':
#     if len(sys.argv) != 3:
#         print("Usage:   index_collection_granules.py collection-catalog-xml-url collection-name")
#         exit(1)

#     _, collection_xml_url, collection_name = sys.argv
    
#     result = index_collection_granules(collection_name, collection_xml_url)
#     print(result)
