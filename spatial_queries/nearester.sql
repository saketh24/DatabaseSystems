select A.FacilityName, A.Address1, A.geolocation, db2gse.st_distance(db2gse.st_point(-72.993983, 40.824369,1), A.geolocation, 'STATUTE MILE') as distance 
from cse532.facility A, cse532.facilitycertification B
where db2gse.st_contains(db2gse.st_buffer(db2gse.st_point(-72.993983, 40.824369,1),'0.25'), A.geolocation) = 1 
and (A.FacilityID = B.FacilityID and B.AttributeValue = 'Emergency Department')
order by distance 
fetch first 1 rows only;