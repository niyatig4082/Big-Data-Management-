pages = LOAD '/user/ds503/input/CircleNetPage.csv'
USING PigStorage(',')
AS (Id:chararray, NickName:chararray, JobTitle:chararray, RegionCode:chararray, FavoriteHobby:chararray);

f = LOAD '/user/ds503/input/follows.csv'
USING PigStorage(',')
AS (FromWho:chararray, ToWho:chararray, DateofRelation:chararray, Desc:chararray);

g = GROUP f BY ToWho;
fc = FOREACH g GENERATE group AS Id, COUNT(f) AS followers;

j = JOIN pages BY Id LEFT OUTER, fc BY Id;

result = FOREACH j GENERATE
  pages::NickName,
  (fc::followers IS NULL ? 0L : fc::followers) AS followers;

STORE result INTO '/user/ds503/output/taskd' USING PigStorage(',');