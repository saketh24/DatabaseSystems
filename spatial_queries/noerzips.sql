-- with temp(zip) as(
	-- select A.ZipCode 
	-- from cse532.facility A, cse532.facilitycertification B
	-- where (A.facilityID = B.facilityID and B.AttributeValue NOT IN('Emergency Department')))
-- select * from temp;





-- select distinct A.ZipCode from cse532.facility A
-- except
-- select A.ZipCode from cse532.facility A, cse532.facilitycertification B
-- where (A.FacilityID = B.FacilityID and B.AttributeValue = 'Emergency Department');

-- with temp(zip) as(
-- select distinct A.ZipCode from cse532.facility A
-- ), 
-- t1(shape,zip) as
-- (
-- select C.shape, temp.zip from temp
-- inner join cse532.uszip C
-- on temp.zip = C.GEOID10
-- ),
-- t2(ZipCode) as(
-- select A.ZipCode from cse532.facility A, cse532.facilitycertification B,
-- where (A.FacilityID = B.FacilityID and B.AttributeValue = 'Emergency Department')),
-- t3 as(
-- select SHAPE, zip from t1 inner join t2 on
-- t1.zip = t2.ZipCode
-- )
-- select zip from t1, t3
-- where db2gse.st_intersects(t1.shape, t3.shape) = 1;

with temp(zip) as(
select distinct A.ZipCode from cse532.facility A
), 
t1(shape,zip) as
(
select C.shape, A.zip from temp A
inner join cse532.uszip C
on A.zip = C.GEOID10
),
t2(ZipCode) as(
select A.ZipCode from cse532.facility A, cse532.facilitycertification B
where (A.FacilityID = B.FacilityID and B.AttributeValue = 'Emergency Department')),
t3(shape, zip) as(
select A.shape, A.zip from t1 A inner join t2 B on
A.zip = B.ZipCode
)
select * from t3 fetch first 1 rows only;
--select zip from t1 A, t3 B
--where db2gse.st_intersects(A.shape, B.shape) = 1;