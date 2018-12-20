from .siphon.catalog import TDSCatalog, Dataset, CatalogRef

# from .timestamp_util import timestamp_re
# from .collection_generator import CollectionGenerator
from . import siphon_ext

from .util import slugify, http_getfile
from .timestamp_util import timestamp_re

from .xml_editor import XMLEditor


from queue import Queue


class GranuleScraper():
    def __init__(self, output_dir):
        self.queue = Queue(maxsize=0)
        self.download_dir = output_dir


    def add_queue_item(self, item):
        self.queue.put(item)


    def process_queued_item(self, item):
        if(type(item) == TDSCatalog):
            self.process_catalog(item)
        elif(type(item) == CatalogRef):
            self.process_catalog_ref(item)
        elif(type(item) == Dataset):
            self.process_dataset(item)


    def process_catalog(self, catalog):
        print("process catalog " + catalog.catalog_url)
        for ds_name, ds in catalog.datasets.items():
            self.add_queue_item(ds)
 
        for ref_name, ref in catalog.catalog_refs.items():
            self.add_queue_item(ref)

    def process_catalog_ref(self, catalog_ref):
        print("process catalog_ref " + catalog_ref.href)
        catalog = catalog_ref.follow()
        self.add_queue_item(catalog)

    def process_dataset(self, ds):
        print("process dataset id %s" % ds.id)
        file = self.dataset_download_file(ds)

        http_getfile(ds.iso_md_url, file)
        self.correct_metadata_file(ds, file)



    def dataset_download_file(self, dataset):
        # collection_name = re.sub(timestamp_re.date_time, '', dataset.name)
        file = self.download_dir + "/" + slugify(dataset.name) + ".iso.xml"
        return(file)

    def correct_metadata_file(self, ds, file):
        xml = XMLEditor.fromfile(file)
        xml.update_xpath_text('/gmi:MI_Metadata/gmd:fileIdentifier/gco:CharacterString', ds.id)
        xml.tofile(file)

    # def harvest_catalog_granules(self, catalog_ref):
    #     catalog = catalog_ref.follow()
        
    #     print("harvest from %s" % catalog.catalog_url)
        
    #     for ds_name, ds in catalog.datasets.items():
    #         if(timestamp_re.search_date_time(ds.id)):

    # def scrape_catalog(self, catalog):
    #     print("scrape " + catalog.catalog_url)
    #     print(catalog.catalog_refs)

