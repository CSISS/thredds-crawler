import re
import itertools
from queue import Queue

from ..siphon.catalog import TDSCatalog, Dataset
from ..filters.granule_to_collection import GranuleToCollection
from ..index import Index

from .base import BaseScraper
from .simple import SimpleScraper


from ..util.path import slugify, mkdir_p
from ..util.http import http_getfile
from ..util.dataset import dataset_set_collection
from ..util.dtutil import timestamp_re

from threading import Thread, Lock
import threading

mutex = Lock()


class CollectionImportScraper(SimpleScraper):
    def __init__(self, output_dir, index):
        super().__init__(output_dir)

        self.tmp_dir = output_dir + '.tmp'
        mkdir_p(self.tmp_dir)

        self.collection_generator = GranuleToCollection(output_dir=output_dir)
        self.index = index

        self.collection_granules = []

    def sync_index(self):
        collections = {}
        for g in self.collection_granules:
            collections.setdefault(g['collection_name'], []).append(g)
        
        print("index sync started")
        # reset the index for each collection
        for collection_name, granules in collections.items():
            print("clear granules for %s" % collection_name)
            self.index.clear_collection(collection_name)
            print("insert %d granules" % len(granules))
            for g in granules:
                self.index.add_granule(g)

        print("index sync complete")

    def process_catalog_ref(self, catalog_ref):
        catalog = catalog_ref.follow()
        self.process_catalog(catalog)



    def process_catalog(self, catalog):
        print("{p Cat} " + catalog.catalog_url)
        for ds_name, ds in catalog.datasets.items():
            dataset_set_collection(ds, catalog)

            if ds.collection_name:
                # this dataset belongs to a collection, we don't want to add it to the threaded queue
                with mutex:
                    # print("do collection %s " % threading.current_thread().name)
                    self.process_collection_dataset(ds)
            else:
                # let simple scraper harvest this dataset later
                self.add_queue_item(ds)
 
        for ref_name, ref in catalog.catalog_refs.items():
            self.add_queue_item(ref)
        
        # print("{DONE Cat}" + catalog.catalog_url)
   

    def process_collection_dataset(self, ds):
        if not self.index.has_collection_for_granule(ds):
            print("add collection " + ds.collection_name)
            self.index.create_collection_for_granule(ds)

            self.create_collection_csw_record(ds)

        # collect all granules we see
        granule = self.index.build_granule(ds)
        self.collection_granules.append(granule)

    def create_collection_csw_record(self, ds):
        file = self.download_dataset(ds, directory=self.tmp_dir)

        self.collection_generator.generate_collection_iso_for_dataset(ds, file)

