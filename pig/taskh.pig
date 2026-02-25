pages = LOAD '/user/ds503/input/CircleNetPage.csv'
USING PigStorage(',')
AS (Id:chararray, NickName:chararray, JobTitle:chararray, RegionCode:chararray, FavoriteHobby:chararray);

f = LOAD '/user/ds503/input/follows.csv'
USING PigStorage(',')
AS (ColRel:chararray, ID1:chararray, ID2:chararray, DateofRelation:chararray, Desc:chararray);

pairs = FOREACH f GENERATE ID1 AS A, ID2 AS B;
rev = FOREACH f GENERATE ID2 AS A, ID1 AS B;

j = JOIN pairs BY (A,B) LEFT OUTER, rev BY (A,B);

oneway = FILTER j BY rev::A IS NULL;

pA = JOIN oneway BY pairs::A, pages BY Id;
pAB = JOIN pA BY oneway::pairs::B, pages BY Id;

sameRegion = FILTER pAB BY pA::pages::RegionCode == pages::RegionCode;

result = FOREACH sameRegion GENERATE
  pA::pages::Id, pA::pages::NickName,
  pages::Id, pages::NickName;

STORE result INTO '/user/ds503/output/taskh' USING PigStorage(',');