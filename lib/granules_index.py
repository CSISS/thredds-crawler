import sys
assert sys.version_info >= (3,5)

import datetime
import json


from .scraper_driver import ScraperDriver
from .scraper.collection_granule_indexer import CollectionGranuleIndexer

from .siphon.catalog import TDSCatalog, Dataset

from .indexdb import IndexDB
from .util.datetime import timestamp_parser



class GranulesIndex():
    def __init__(self, base_dir):
        self.db = IndexDB("sqlite:///" + base_dir + "/thredds_granule_index.db", False)

        # self.db.drop_sql_tables()
        self.db.create_sql_tables()


    def is_index_expired(self, url):
        return True
        margin = datetime.timedelta(minutes = 10)
        cutoff = datetime.datetime.now() - margin

        index_datetime = self.db.get_collection_updated_at(url)
        if index_datetime and index_datetime > cutoff:
            return False
        
        return True


    def update_index(self, catalog_name, catalog_url):
        print("update_index %s" % catalog_url, flush=True)

        
        if not self.is_index_expired(catalog_url):
            print("skipping %s (recent index available)" %  catalog_url)
            return

        indexer = CollectionGranuleIndexer()
        harvester = ScraperDriver(indexer, 40, 10)

        harvester.harvest(catalog_url)
        results = indexer.indexes
        
        self.db.index_collection_granules(catalog_name, catalog_url, results)

        return("discovered %d granules at %s" % (len(results), catalog_url))


    def get_index(self, catalog_url, start_time, end_time):
        start_time = timestamp_parser.parse_datetime(start_time)
        end_time = timestamp_parser.parse_datetime(end_time)

        granules = self.db.get_collection_granules(catalog_url, start_time, end_time)

        for g in granules:
            g['time_start'] = timestamp_parser.to_str(g['time_start'])
            g['time_end'] = timestamp_parser.to_str(g['time_end'])

        return(json.dumps(granules))


