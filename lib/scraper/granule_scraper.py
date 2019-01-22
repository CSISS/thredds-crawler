from ..siphon.catalog import TDSCatalog, Dataset, CatalogRef

from ..util.path import slugify
from ..util.http import http_getfile
from ..util.datetime import timestamp_re
from ..scraper.base_scraper import BaseScraper
from ..processor.xml_editor import XMLEditor


from queue import Queue


class GranuleScraper(BaseScraper):
    def __init__(self, output_dir):
        super().__init__()

        self.download_dir = output_dir


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

        file_identifier = "<gco:CharacterString xmlns:gco=\"http://www.isotc211.org/2005/gco\">%s</gco:CharacterString>" % ds.authority_ns_id
        xml.update_xpath_fromstring('/gmi:MI_Metadata/gmd:fileIdentifier', file_identifier)

        xml.tofile(file)

    # def harvest_catalog_granules(self, catalog_ref):
    #     catalog = catalog_ref.follow()
        
    #     print("harvest from %s" % catalog.catalog_url)
        
    #     for ds_name, ds in catalog.datasets.items():
    #         if(timestamp_re.search_date_time(ds.id)):

    # def scrape_catalog(self, catalog):
    #     print("scrape " + catalog.catalog_url)
    #     print(catalog.catalog_refs)

