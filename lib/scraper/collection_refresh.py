import re
from queue import Queue
import datetime

from ..siphon.catalog import TDSCatalog, Dataset
from ..filters.granule_to_collection import GranuleToCollection
from ..index import Index

from .base import BaseScraper
from .simple import SimpleScraper
from .collection_import import CollectionImportScraper


from ..util.path import slugify, mkdir_p
from ..util.http import http_getfile
from ..util.dtutil import timestamp_re
from ..util.dataset import dataset_process_collection_name


from threading import Thread, Lock
import threading

mutex = Lock()


class CollectionRefreshScraper(BaseScraper):
    def __init__(self, index):
        super().__init__()

        self.index = index

    def set_refresh_scope(self, collection_name, start_time, end_time):
        catalog_url = self.index.get_catalog_url(collection_name)
        if not catalog_url:
            print("[WARN] no catalog in index %s" % catalog_url)
            return None

        if self.is_refresh_needed(collection_name, start_time, end_time):
            self.start_time = start_time
            self.end_time = end_time
            print("REINDEX %s %s" % (collection_name, catalog_url))
            self.add_catalog(catalog_url)

    def is_refresh_needed(self, collection_name, start_time, end_time):
        if 'edu.ucar.rda' in collection_name:
            print("[INFO] no reindex needed for RDA")
            return False

        last_index_time = self.index.last_updated_at(collection_name)
        margin = datetime.timedelta(minutes = 30)
        # margin = datetime.timedelta(minutes = 0)
        
        print(last_index_time)
        print(datetime.datetime.now())

        if (last_index_time > end_time) or (last_index_time + margin > datetime.datetime.now()):
            print("[INFO] index up to date for %s in range (%s to %s)" % (collection_name, start_time, end_time))
            return False

        return True
        


    def process_catalog(self, catalog):
        # print("{p Cat} " + catalog.catalog_url)

        for ds_name, ds in catalog.datasets.items():
            dataset_process_collection_name(ds)
            self.index.add_granule(ds)
 
        for ref_name, ref in catalog.catalog_refs.items():
            self.add_queue_item(ref)

    def process_catalog_ref(self, catalog_ref):
        # print("{p CRef} " + catalog_ref.href)
        catalog = catalog_ref.follow()
        self.add_queue_item(catalog)

    def process_dataset(self, ds):
        return




