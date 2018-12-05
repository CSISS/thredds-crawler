#!/bin/sh

docker build . -t csiss/thredds-harvester-indexer

docker rm -f test-harvester-indexer
# docker run --name test-harvester-indexer -p 8001:8000 --rm -ti csiss/thredds-harvester-indexer
docker run --name test-harvester-indexer -p 8001:8000 --rm -d csiss/thredds-harvester-indexer
exit


curl http://localhost:8001/harvest/granules -X POST
curl http://localhost:8001/harvest/collection -X POST


# docker logs -f test-harvester-indexer
# docker exec -ti test-harvester-indexer /bin/ash

curl http://localhost:8001/granules_index -d "collection_name=SSEC__x__IDD-Satellite__x__IR__x__WEST-CONUS_4km__x__WEST-CONUS_4km_IR&collection_xml_url=http://thredds.ucar.edu/thredds/catalog/satellite/IR/WEST-CONUS_4km/catalog.xml" -X POST


curl 'http://localhost:8001/granules_index?start_time=2018-11-02T23:26:40&end_time=2018-12-30T23:26:40&collection_xml_url=http://thredds.ucar.edu/thredds/catalog/satellite/IR/WEST-CONUS_4km/catalog.xml'