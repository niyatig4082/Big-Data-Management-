pages = LOAD '/user/ds503/input/CircleNetPage.csv'
USING PigStorage(',')
AS (Id:chararray, NickName:chararray, JobTitle:chararray, RegionCode:chararray, FavoriteHobby:chararray);

same = FILTER pages BY FavoriteHobby == 'Swimming';

result = FOREACH same GENERATE NickName, JobTitle;

STORE result INTO '/user/ds503/output/taskc' USING PigStorage(',');