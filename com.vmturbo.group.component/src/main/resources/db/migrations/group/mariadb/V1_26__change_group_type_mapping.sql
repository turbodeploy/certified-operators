-- This migration is aimed to change GroupType mapped by ordinal to getNumer()

UPDATE grouping set group_type = group_type + 1 where group_type > 0;