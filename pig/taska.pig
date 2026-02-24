pages = LOAD '/user/ds503/input/CircleNetPage.csv' 
USING PigStorage(',') 
AS (Id:chararray, NickName:chararray, JobTitle:chararray, RegionCode:int, FavoriteHobby:chararray);

grp = GROUP pages BY FavoriteHobby;
counts = FOREACH grp GENERATE group AS FavoriteHobby, COUNT(pages) AS frequency;

STORE counts INTO '/user/ds503/output/taskA' USING PigStorage(',');