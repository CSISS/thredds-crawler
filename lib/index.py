import sys
assert sys.version_info >= (3,5)

import datetime
import json

from .scraper_driver import ScraperDriver
# from .scraper.collection_granule_indexer import CollectionGranuleIndexer

from .siphon.catalog import TDSCatalog, Dataset

from .indexdb import IndexDB
from .util.dtutil import timestamp_parser, time_coverage_to_time_span
from .util.dataset import dataset_access_url


class Index():
    def __init__(self, index_file):
        db_file = "sqlite:///" + index_file
        print("database=" + db_file)
        self.db = IndexDB(db_file, False)

        # self.db.drop_sql_tables()
        self.db.create_sql_tables()


    def last_updated_at(self, collection_name):
        return self.db.get_collection_updated_at(name=collection_name)

    def get_catalog_url(self, collection_name):
        return self.db.get_collection_catalog_url(collection_name)

    def get_latest_granule_time(collection_name, catalog_url):
        col = self.db.find_collection(name=ds.collection_name, url=catalog_url)

    def has_collection_for_granule(self, ds):
        col = self.db.find_collection(name=ds.collection_name)
        return(col)

    def create_collection_for_granule(self, ds):
        self.db.create_collection(name=ds.collection_name, url=ds.collection_catalog_url)

    def clear_collection(self, collection_name):
        self.db.delete_collection_granules(collection_name)

    def add_granule(self, ds):
        access_url = dataset_access_url(ds)

        start, end = time_coverage_to_time_span(**ds.time_coverage)
        granule = {'name': ds.authority_ns_id, 'iso_url': ds.iso_md_url, 'access_url': access_url, 'time_start': start, 'time_end': end}
        
        cid = self.db.find_collection(name=ds.collection_name)
        self.db.add_collection_granule(cid, granule)

    def get_granules(self, collection_name, start_time, end_time):
        granules = self.db.get_collection_granules(collection_name, start_time, end_time)

        for g in granules:
            g['time_start'] = timestamp_parser.to_str(g['time_start'])
            g['time_end'] = timestamp_parser.to_str(g['time_end'])

        return(granules)


