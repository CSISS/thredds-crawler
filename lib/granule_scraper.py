from .siphon.catalog import TDSCatalog, Dataset

# from .timestamp_util import timestamp_re
# from .collection_generator import CollectionGenerator
from . import siphon_ext

from .util import slugify, http_getfile
from .timestamp_util import timestamp_re

from .xml_editor import XMLEditor


from queue import Queue


class GranuleScraper():
    def __init__(self):
        self.queue = Queue(maxsize=0)
        self.download_dir = '../records/granules'

    def dataset_download_file(self, dataset):
        # collection_name = re.sub(timestamp_re.date_time, '', dataset.name)
        file = self.download_dir + "/" + slugify(dataset.name) + ".iso.xml"
        return(file)

    def correct_metadata_file(self, ds, file):
        xml = XMLEditor.fromfile(file)
        xml.update_xpath_text('/gmi:MI_Metadata/gmd:fileIdentifier/gco:CharacterString', ds.id)
        xml.tofile(file)

    def harvest_catalog_granules(self, catalog_ref):
        catalog = catalog_ref.follow()
        
        print("harvest from %s" % catalog.catalog_url)
        
        for ds_name, ds in catalog.datasets.items():
            if(timestamp_re.search_date_time(ds.id)):
                url = catalog.iso_md_url(ds)
                file = self.dataset_download_file(ds)
                print("dataset id %s" % ds.id)

                http_getfile(url, file)
                self.correct_metadata_file(ds, file)

    def scrape_catalog(self, catalog):
        for ref_name, ref in catalog.catalog_refs.items():

            # if catalog contains sub catalog named like: "20180818"
            if(timestamp_re.fullmatch_yesterday(ref_name)):
                self.harvest_catalog_granules(ref)
                return

            # if catalog contains sub-catalog with date and time in the name
            # like: "GEFS_Global_1p0deg_Ensemble_20180818_0000.grib2"
            if(timestamp_re.search_date_time(ref_name)):
                self.harvest_catalog_granules(ref)
                return

            # process subcatalogs that don't contain a timestamp
            if(timestamp_re.search_date(ref_name) == None):
                self.queue.put(ref)