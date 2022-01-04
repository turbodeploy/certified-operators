-- changing column in scope table to make it much clearer which side of the scoping relationship
-- each of the two identified entities are on.
--
-- Specificaly: if there's a scope table record with seed_oid (fka entity_oid) = e1 and scoped_oid
-- = e2, then e2 is in the scope of e1, but not necessarily vice-versa. This means that a supply
-- chain traversal seeded with e1 visits e2.
ALTER TABLE scope RENAME COLUMN entity_oid TO seed_oid;
