import re
from .dtutil import timestamp_re

u = r"[\._/-]"
nd = r"(?!\d)"
series_member_re = re.compile(u + timestamp_re.date_time + nd + '|' + u + timestamp_re.date + nd)

# Level3_Composite_ntp_4km_20190218_1735.gini           -> Level3_Composite_ntp_4km.gini
# NDFD_NWS_CONUS_2p5km_20190119_0000.grib2              -> NDFD_NWS_CONUS_2p5km.grib2
# GEFS_Global_1p0deg_Ensemble_ana_20190119_0000.grib2   -> GEFS_Global_1p0deg_Ensemble_ana.grib2
# asr15km.fix.2000010100.XLONG.nc                       -> asr15km.fix.XLONG.nc
# asr15km.anl.3D.20000106.nc                            -> asr15km.anl.3D.nc
# asr15km.fix.2000002100.XLONG.nc                       -> None  ## This looks like a timestamp but is not a timestamp
def dataset_collection_name(ds):
    name = ds.authority_ns_id
    if re.search(series_member_re, name):
        return re.sub(series_member_re, '', name)
    return None


def dataset_collection_catalog_url(ds):
    # if catalog name contains a date then ds.catalog is not top level catalog
    if timestamp_re.search_date(ds.catalog.ref_name) and ds.catalog.parent_catalog:
        return ds.catalog.parent_catalog.catalog_url
    return ds.catalog.catalog_url

def dataset_process_collection_name(ds):

    ds.collection_name = dataset_collection_name(ds)

    # print("*** %s ---> %s" % (ds.authority_ns_id, ds.collection_name ))
    if ds.collection_name:
        ds.collection_catalog_url = dataset_collection_catalog_url(ds)
