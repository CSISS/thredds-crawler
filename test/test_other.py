from lib.siphon.catalog import TDSCatalog, Dataset
# tc = TDSCatalog('http://thredds.ucar.edu/thredds/catalog.xml')
# c1 = tc.follow_refs('Radar Data', 'NEXRAD Level III Radar', 'PTA', 'YUX', '20180930')
# c2 = TDSCatalog('http://thredds.ucar.edu/thredds/catalog/satellite/IR/WEST-CONUS_4km/20180920/catalog.xml')
# c3 = TDSCatalog('http://thredds.ucar.edu/thredds/catalog/grib/NCEP/GEFS/Global_1p0deg_Ensemble/members-analysis/GEFS_Global_1p0deg_Ensemble_ana_20180930_0000.grib2/catalog.xml')
# print(cat.datasets[0].time_coverage)

# from lib.indexdb import IndexDB

# print(IndexDB.create_sql_tables())

# pgcli -h localhost -p 8432 pycsw postgres
# CREATE DATABASE thredds_granule_index
db = IndexDB('postgresql://postgres:mypass@localhost:8432/thredds_granule_index')
# db = IndexDB('postgresql://postgres:mypass@localhost:8432/pycsw')
# rows = db.query('SELECT datname FROM pg_database')

# db.drop_sql_tables()
# db.create_sql_tables()

