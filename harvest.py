import sys
assert sys.version_info >= (3,5)

import os

import pathlib


from lib.threaded_harvester import ThreadedHarvester
from lib.collection_scraper import CollectionScraper
from lib.granule_scraper import GranuleScraper

from lib.siphon.catalog import TDSCatalog, Dataset



def lock_fname(item_type):
    fname = "/tmp/%s_harvest.lock" % item_type
    return(fname)


def create_lock(item_type):
    fname = lock_fname(item_type)
    with open(fname, 'a'):
        os.utime(fname, None)


def delete_lock(item_type):
    fname = lock_fname(item_type)
    if(exists_lock(item_type)):
        os.remove(fname)

def exists_lock(item_type):
    fname = lock_fname(item_type)
    return(os.path.isfile(fname))


def mkdir_p(dirpath):
    pathlib.Path(dirpath).mkdir(parents=True, exist_ok=True)


def do_harvest(catalog_url, harvest_type, job_id):
    # PSEUDOCODE
    cat = TDSCatalog(catalog_url)



    if harvest_type == 'granules':
        output_dir = RECORDS_DIR + '/granules.' + job_id
        mkdir_p(output_dir)

        scraper = GranuleScraper(output_dir)
    else:
        output_dir = RECORDS_DIR + '/collection.' + job_id
        tmp_dir = output_dir + '.tmp'
        mkdir_p(output_dir)
        mkdir_p(tmp_dir)

        scraper = CollectionScraper(output_dir, tmp_dir)

    harvester = ThreadedHarvester(scraper, 20, 1)
    harvester.harvest(cat.catalog_refs.values())

    return output_dir





    # try:
    #     create_lock(item_type)

    #     refs = {}
    #     top_cat = TDSCatalog('http://thredds.ucar.edu/thredds/catalog.xml')

    #     # GRANULES
    #     if item_type == 'granules':
    #         scraper = GranuleScraper(records_dir = RECORDS_DIR)
    #         cat = top_cat.catalog_refs['Satellite Data'].follow()
    #         refs['goes16'] = cat.catalog_refs['GOES16']
    #         refs['goes16grb'] = cat.catalog_refs['GOES16 GRB']
    #         refs['goes17'] = cat.catalog_refs['GOES17']

    #         # refs['forecast'] = top_cat.catalog_refs['Forecast Model Data']
    #         # # refs['satellite infrared'] = top_cat.catalog_refs['Satellite Data'].follow().catalog_refs['Infrared (11 um)']

    #         # # refs['nexrad2-tjua'] = top_cat.catalog_refs['Radar Data'].follow() \
    #         # #             .catalog_refs['NEXRAD Level II Radar WSR-88D'].follow() \
    #         # #             .catalog_refs['TJUA']

    #         # refs['nexrad3-pta-yux'] = top_cat.catalog_refs['Radar Data'].follow() \
    #         #             .catalog_refs['NEXRAD Level III Radar'].follow()\
    #         #             .catalog_refs['PTA'].follow() \
    #         #             .catalog_refs['YUX']


    #     # COLLECTIONS
    #     else:
    #         scraper = CollectionScraper(records_dir = RECORDS_DIR)

    #         # change
    #         # refs['forecast'] = top_cat.catalog_refs['Forecast Model Data']
    #         refs['satellite infrared'] = top_cat.catalog_refs['Satellite Data'].follow().catalog_refs['Infrared (11 um)']

    #         refs['nexrad2-tjua'] = top_cat.catalog_refs['Radar Data'].follow() \
    #                     .catalog_refs['NEXRAD Level II Radar WSR-88D']

 
    #     harvester = ThreadedHarvester(scraper, 40, 10)

    #     harvester.harvest(refs.values())
    #     print("done harvesting " + item_type)
    # finally:
    #     delete_lock(item_type)

RECORDS_DIR = 'records'

if __name__ == '__main__':
    do_harvest(sys.argv[1])