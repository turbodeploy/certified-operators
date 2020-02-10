UPDATE group_static_members_entities SET entity_type = 64 WHERE entity_type = 33 OR entity_type = 24;

DELETE FROM group_expected_members_entities
WHERE entity_type = 33 AND group_id IN (
  SELECT DISTINCT a.group_id FROM group_expected_members_entities as a
  LEFT OUTER JOIN (
    SELECT group_id, entity_type, count(entity_type) as count_of_types
    FROM group_expected_members_entities
    WHERE entity_type = 33 OR entity_type = 24
    GROUP BY group_id) as c
  ON  c.group_id = a.group_id
  WHERE a.entity_type = 33 AND count_of_types > 1);

UPDATE group_expected_members_entities SET entity_type = 64 WHERE entity_type = 33 OR entity_type = 24;
