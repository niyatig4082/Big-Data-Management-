pages = LOAD '/user/ds503/input/CircleNetPage.csv'
USING PigStorage(',')
AS (Id:chararray, NickName:chararray, JobTitle:chararray, RegionCode:chararray, FavoriteHobby:chararray);

f = LOAD '/user/ds503/input/follows.csv'
USING PigStorage(',')
AS (ColRel:chararray, ID1:chararray, ID2:chararray, DateofRelation:chararray, Desc:chararray);

g = GROUP f BY ID2;
fc = FOREACH g GENERATE group AS Id, COUNT(f) AS followers;

j = JOIN pages BY Id LEFT OUTER, fc BY Id;

pf = FOREACH j GENERATE
  pages::Id AS Id,
  pages::NickName AS NickName,
  (fc::followers IS NULL ? 0L : fc::followers) AS followers;

allpf = GROUP pf ALL;
avgRel = FOREACH allpf GENERATE AVG(pf.followers) AS avgFollowers;

joined = CROSS pf, avgRel;

above = FILTER joined BY followers > avgFollowers;

out = FOREACH above GENERATE NickName, followers;

STORE out INTO '/user/ds503/output/taskf' USING PigStorage(',');