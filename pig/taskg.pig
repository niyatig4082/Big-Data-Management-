pages = LOAD '/user/ds503/input/CircleNetPage.csv'
USING PigStorage(',')
AS (Id:chararray, NickName:chararray, JobTitle:chararray, RegionCode:chararray, FavoriteHobby:chararray);

log = LOAD '/user/ds503/input/ActivityLog.csv'
USING PigStorage(',')
AS (ActionId:chararray, ByWho:chararray, WhatPage:chararray, ActionType:chararray, ActionTime:chararray);

logt = FOREACH log GENERATE ByWho AS Id, (long)ActionTime AS t;

g1 = GROUP logt BY Id;
last = FOREACH g1 GENERATE group AS Id, MAX(logt.t) AS lastTime;

gAll = GROUP logt ALL;
mx = FOREACH gAll GENERATE MAX(logt.t) AS maxTime;

th = FOREACH mx GENERATE (maxTime - (90L*24L*60L*60L)) AS threshold;

j = JOIN pages BY Id LEFT OUTER, last BY Id;
j2 = CROSS j, th;

outdated = FILTER j2 BY (last::lastTime IS NULL) OR (last::lastTime < threshold);

result = FOREACH outdated GENERATE pages::Id, pages::NickName;

STORE result INTO '/user/ds503/output/taskg' USING PigStorage(',');