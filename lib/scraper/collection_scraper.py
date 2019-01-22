import re
from queue import Queue

from ..siphon.catalog import TDSCatalog, Dataset

from ..filters.granule_to_collection import GranuleToCollection
from .base_scraper import BaseScraper

from ..util.path import slugify
from ..util.http import http_getfile
from ..util.datetime import timestamp_re


class CollectionScraper(BaseScraper):
    def __init__(self, output_dir, tmp_dir):
        super().__init__()

        self.tmp_dir = tmp_dir
        self.collection_generator = GranuleToCollection(output_dir=output_dir)

    def process_catalog(self, catalog):
        print("process catalog " + catalog.catalog_url)

        for ref_name, ref in catalog.catalog_refs.items():

            # if catalog contains sub catalog named like: "20180818"
            if(timestamp_re.fullmatch_yesterday(ref_name)):
                self.harvest_collection_catalog_metadata(catalog, ref)
                return

            # if catalog contains sub-catalog with date and time in the name
            # like: "GEFS_Global_1p0deg_Ensemble_20180818_0000.grib2"
            if(timestamp_re.search_date_time(ref_name)):
                self.harvest_collection_catalog_metadata(catalog, ref)
                return

            # process subcatalogs that don't contain a timestamp
            if(timestamp_re.search_date(ref_name) == None):
                self.add_queue_item(ref)
            

    def process_catalog_ref(self, catalog_ref):
        print("process catalog_ref " + catalog_ref.href)
        catalog = catalog_ref.follow()
        self.add_queue_item(catalog)

    def process_dataset(self, ds):
        return


    def dataset_download_url(self, dataset_catalog, dataset):
        return(dataset_catalog.iso_md_url(dataset))

    def dataset_download_file(self, dataset):
        # collection_name = re.sub(timestamp_re.date_time, '', dataset.name)
        file = self.tmp_dir + "/" + slugify(dataset.name) + ".iso.xml"
        return(file)


    def harvest_collection_catalog_metadata(self, collection_catalog, leaf_ref):
        leaf_catalog = leaf_ref.follow()



        for ds_name, ds in leaf_catalog.datasets.items():
            if(timestamp_re.search_date_time(ds.id)):
                url = self.dataset_download_url(leaf_catalog, ds)
                file = self.dataset_download_file(ds)

                http_getfile(url, file)

                self.collection_generator.generate_collection_iso_for_dataset(collection_catalog, ds, url, file)

                return


    # def scrape_catalog(self, catalog):
        # print("TRAVERSE  %s" % catalog.ref_name)

