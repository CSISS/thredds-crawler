import re
from queue import Queue

from ..siphon.catalog import TDSCatalog, Dataset
from ..filters.granule_to_collection import GranuleToCollection
from ..index import Index

from .base import BaseScraper
from .simple import SimpleScraper


from ..util.path import slugify, mkdir_p
from ..util.http import http_getfile
from ..util.datetime import timestamp_re
from ..util.dataset import dataset_process_collection_name


from threading import Thread, Lock
import threading

mutex = Lock()


class CollectionScraper(SimpleScraper):
    def __init__(self, output_dir):
        super().__init__(output_dir)

        self.tmp_dir = output_dir + '.tmp'
        mkdir_p(self.tmp_dir)

        self.collection_generator = GranuleToCollection(output_dir=output_dir)
        self.index = Index(output_dir)


    def process_catalog(self, catalog):
        # print("{p Cat} " + catalog.catalog_url)
        for ds_name, ds in catalog.datasets.items():
            dataset_process_collection_name(ds)
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
   

    def process_collection_dataset(self, ds):
        if not self.index.has_collection_for_granule(ds):
            print("--- %s no collection %s" %(threading.current_thread().name, ds.collection_name))
            self.index.create_collection_for_granule(ds)
            print("+++ %s created collection %s" %(threading.current_thread().name, ds.collection_name))

            self.create_collection_csw_record(ds)

        self.index.add_granule(ds)


    def create_collection_csw_record(self, ds):
        file = self.download_dataset(ds, directory=self.tmp_dir)

        self.collection_generator.generate_collection_iso_for_dataset(ds, file)

