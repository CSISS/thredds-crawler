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

import datetime

import time


class CollectionImportScraper(SimpleScraper):
    def __init__(self, output_dir, index):
        super().__init__(output_dir)

        self.counter = 0

        self.tmp_dir = output_dir + '.tmp'
        mkdir_p(self.tmp_dir)

        self.collection_generator = GranuleToCollection(output_dir=output_dir)
        self.index = index

        self.collection_granules = {}
        self.collection_datasets = {}

    def sync_index(self):
        t = time.time()
        print("index sync started")

        for collection_name, ds in self.collection_datasets.items():
            print("add collection " + collection_name)
            self.index.create_collection_for_granule(ds)


        for collection_name, granules in self.collection_granules.items():
            print("%d granules for %s" % (len(granules), collection_name))
            self.index.clear_collection(collection_name)
            # for g in granules:
            self.index.add_granules(collection_name, granules)

        print("index sync complete (%.2f seconds)" % (time.time() - t), flush=True)

    def process_catalog_ref(self, catalog_ref):
        tstart = time.time()
        catalog = catalog_ref.follow()
        td = time.time() - tstart


        print("DL %.2f %s" % (td, catalog_ref.href))

        self.process_catalog(catalog)



    def process_catalog(self, catalog):
        for ds_name, ds in catalog.datasets.items():
            dataset_set_collection(ds, catalog)

            if ds.collection_name:
                self.process_collection_dataset(ds)

        # QI - Queue Item
        tstart = time.time()
        for ref_name, ref in catalog.catalog_refs.items():
            self.add_queue_item(ref)
        td = time.time() - tstart
        # print("QI %.2f %s" % (td, catalog.catalog_url))
        

    def process_collection_dataset(self, ds):
        ## AG - Append Granule and DS
        t = time.time()
        
        collection_name = ds.collection_name
        granule = self.index.build_granule(ds)
        # print(collection_name)

        if(not collection_name in self.collection_datasets):
            self.collection_datasets[collection_name] = ds
            try:
                self.create_collection_csw_record(ds)
            except Exception as e:
                print(e)
                print("[WARN] Failed to get dataset %s ISO metadta. It will not appear in CSW" % ds.authority_ns_id)


        self.collection_granules.setdefault(collection_name, []).append(granule)

        # print("AG %.2f %s" % (time.time() - t, ds.id), flush=True)



    def create_collection_csw_record(self, ds):
        tstart = time.time()

        file = self.download_dataset(ds, directory=self.tmp_dir)

        self.collection_generator.generate_collection_iso_for_dataset(ds, file)

        td = time.time() - tstart
        print("C %.2f %s" % (td, ds.collection_name))

