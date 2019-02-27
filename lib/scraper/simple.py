from ..siphon.catalog import TDSCatalog, Dataset, CatalogRef

from ..util.path import slugify, mkdir_p
from ..util.http import http_getfile
from ..util.dtutil import timestamp_re

from .base import BaseScraper

from ..filters.xml_editor import XMLEditor
from ..filters.granule import CommonFilter, RDAFilter

from queue import Queue


class SimpleScraper(BaseScraper):
    def __init__(self, output_dir):
        super().__init__()

        self.download_dir = output_dir
        mkdir_p(self.download_dir)

    def process_catalog(self, catalog):
        print("{p Cat} " + catalog.catalog_url)
        for ds_name, ds in catalog.datasets.items():
            self.add_queue_item(ds)
 
        for ref_name, ref in catalog.catalog_refs.items():
            self.add_queue_item(ref)

    def process_catalog_ref(self, catalog_ref):
        # print("{p CRef} " + catalog_ref.href)
        catalog = catalog_ref.follow()
        self.add_queue_item(catalog)

    def process_dataset(self, ds):
        print("{p DS} %s" % ds.id)

        file = self.download_dataset(ds, self.download_dir)

        self.apply_filter(CommonFilter, ds, file)
        self.apply_filter(RDAFilter, ds, file)


    def download_dataset(self, ds, directory=None):
        if directory == None:
            directory = self.download_dir

        file = directory + "/" + slugify(ds.authority_ns_id) + ".iso.xml"
        http_getfile(ds.iso_md_url, file)
        return(file)



    # def harvest_catalog_granules(self, catalog_ref):
    #     catalog = catalog_ref.follow()
        
    #     print("harvest from %s" % catalog.catalog_url)
        
    #     for ds_name, ds in catalog.datasets.items():
    #         if(timestamp_re.search_date_time(ds.id)):

    # def scrape_catalog(self, catalog):
    #     print("scrape " + catalog.catalog_url)
    #     print(catalog.catalog_refs)

