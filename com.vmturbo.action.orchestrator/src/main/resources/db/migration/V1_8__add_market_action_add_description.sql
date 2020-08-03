-- Adding description to market_action table to persist the action description when executing plans.
ALTER TABLE market_action ADD COLUMN description varchar(510) DEFAULT NULL AFTER recommendation;