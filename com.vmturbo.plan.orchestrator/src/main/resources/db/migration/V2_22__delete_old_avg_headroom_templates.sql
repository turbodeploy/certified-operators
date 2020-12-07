-- delete old avg headroom templates
DELETE FROM template where name like 'AVG:%for last 10 days';