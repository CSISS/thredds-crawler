import sys, os
assert sys.version_info >= (3,6)

from datetime import datetime, timedelta

# relative path for loading lib modules
sys.path.append(os.path.abspath('.'))
sys.path.append(os.path.abspath('..'))

from lib.indexdb import IndexDB
from lib.config import config


db = IndexDB(config['index_db_url'], True)

db.drop_sql_tables()
db.create_sql_tables()




delta = timedelta(minutes=10)

s1 = datetime.now()
e1 = s1 + delta

s2 = e1 + timedelta(hours=1)
e2 = s2 + delta

granules = []
granules.append({'name': 'granule1', 
    'iso_url': 'http://thredd.ucar.edu/catalog/granule1.xml', 
    'access_url': 'http://thredd.ucar.edu/files/g1.nc', 
    'time_start': s1, 
    'time_end': e1})

granules.append({'name': 'granule2', 
    'iso_url': 'http://thredd.ucar.edu/catalog/granule2.xml', 
    'access_url': 'http://thredd.ucar.edu/files/g2.nc', 
    'time_start': s2, 
    'time_end': e2})


db.index_collection_granules('col_name1', 'col_url1', granules)

db.index_collection_granules('col_name1', 'col_url1', granules)

db.index_collection_granules('col_name1', 'col_url1', granules)

# granule 1
print(db.get_collection_granules('col_url1', s1, s2))



# granules 1 & 2
print(db.get_collection_granules('col_url1', s1, e2))