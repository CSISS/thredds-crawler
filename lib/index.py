import sys
assert sys.version_info >= (3,5)

import datetime
import json

from .scraper_driver import ScraperDriver
# from .scraper.collection_granule_indexer import CollectionGranuleIndexer

from .siphon.catalog import TDSCatalog, Dataset

from .indexdb import IndexDB
from .util.datetime import timestamp_parser, time_coverage_to_time_span


class Index():
    def __init__(self, base_dir):
        db_file = "sqlite:///" + base_dir + "/thredds_granule_index.db"
        print("database=" + db_file)
        self.db = IndexDB(db_file, False)

        # self.db.drop_sql_tables()
        self.db.create_sql_tables()


    def is_expired(self, collection_name):
        return True
        margin = datetime.timedelta(minutes = 10)
        cutoff = datetime.datetime.now() - margin

        index_datetime = self.db.get_collection_updated_at(name=collection_name)
        if index_datetime and index_datetime > cutoff:
            return False
        
        return True


    # TODO: make this work
    def reindex(self, collection_name=None, collection_catalog_url=None):
        print("reindex %s" % name, flush=True)
        
        if not self.is_expired(collection_name):
            print("skipping %s (recent index available)" %  collection_name)
            return

        indexer = CollectionGranuleIndexer()
        harvester = ScraperDriver(indexer, 40, 10)

        harvester.harvest(catalog_url)
        results = indexer.indexes
        
        cid = self.db.find_collection(name=collection_name, url=collection_catalog_url)
        for granule in results:
            self.db.add_collection_granule(cid, granule)

        return("discovered %d granules at %s" % (len(results), catalog_url))

    def has_collection_for_granule(self, ds):
        col = self.db.find_collection(name=ds.collection_name)
        # print("FOUND %s" % col)
        return(col)

    def create_collection_for_granule(self, ds):
        print("INSERT %s " % ds.collection_name)
        self.db.create_collection(name=ds.collection_name, url=ds.collection_catalog_url)

    def add_granule(self, ds):
        # print("index ds %s" % ds.id)
        # print(ds.time_coverage)
        access_url = ds.access_urls.get('HTTPServer') or ds.access_urls.get('OPENDAP')

        start, end = time_coverage_to_time_span(**ds.time_coverage)
        granule = {'name': ds.authority_ns_id, 'iso_url': ds.iso_md_url, 'access_url': access_url, 'time_start': start, 'time_end': end}
        
        cid = self.db.find_collection(name=ds.collection_name)
        self.db.add_collection_granule(cid, granule)

    def get_index_entries(self, collection_id, start_time, end_time):
        start_time = timestamp_parser.parse_datetime(start_time)
        end_time = timestamp_parser.parse_datetime(end_time)

        granules = self.db.get_collection_granules(catalog_url, start_time, end_time)

        for g in granules:
            g['time_start'] = timestamp_parser.to_str(g['time_start'])
            g['time_end'] = timestamp_parser.to_str(g['time_end'])

        return(json.dumps(granules))


