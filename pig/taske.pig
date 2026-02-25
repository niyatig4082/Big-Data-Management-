log = LOAD '/user/ds503/input/ActivityLog.csv'
USING PigStorage(',')
AS (ActionId:chararray, ByWho:chararray, WhatPage:chararray, ActionType:chararray, ActionTime:chararray);

g = GROUP log BY WhatPage;

result = FOREACH g {
  u = DISTINCT log.ByWho;
  GENERATE
    group AS PageId,
    COUNT(log) AS totalActions,
    COUNT(u) AS distinctUsers;
};

STORE result INTO '/user/ds503/output/taske' USING PigStorage(',');