pages = LOAD '/user/ds503/input/CircleNetPage.csv'
USING PigStorage(',')
AS (Id:chararray, NickName:chararray, JobTitle:chararray, RegionCode:chararray, FavoriteHobby:chararray);

log = LOAD '/user/ds503/input/ActivityLog.csv'
USING PigStorage(',')
AS (ActionId:chararray, ByWho:chararray, WhatPage:chararray, ActionType:chararray, ActionTime:chararray);

g = GROUP log BY WhatPage;

counts = FOREACH g GENERATE group AS PageId, COUNT(log) AS numVisits;

sorted = ORDER counts BY numVisits DESC;
top10 = LIMIT sorted 10;

j = JOIN top10 BY PageId, pages BY Id;

result = FOREACH j GENERATE pages::Id, pages::NickName, pages::JobTitle;

STORE result INTO '/user/ds503/output/taskb' USING PigStorage(',');
