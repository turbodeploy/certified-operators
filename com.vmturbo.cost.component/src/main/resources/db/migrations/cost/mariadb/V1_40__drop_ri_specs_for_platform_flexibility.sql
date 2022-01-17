-- Dropping all data related to RI specs is necessary as part of OM-56482. This bug added platform
-- flexibility to RI data discovered. Given both RI inventory and RI buy recommendations may refer
-- to RI specs in which the platform flexibility flag is incorrectly set, this data must be cleared
-- from the DB. It is expected Buy RI recommendations will regenerate, RI inventory will be re-uploaded
-- to the cost component, and OCP-support has not been release.

TRUNCATE TABLE reserved_instance_bought;

TRUNCATE TABLE buy_reserved_instance;

TRUNCATE TABLE plan_reserved_instance_bought;

-- cannot truncate due to foreign key constraint on plan_reserved_instance_bought and buy_reserved_instance.
DELETE FROM reserved_instance_spec;