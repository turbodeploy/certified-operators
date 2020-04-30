-- A registered component is a particular instance of a particular component type - e.g.
-- a specific topology processor pod - that's active in the deployment.
--
-- The cluster manager hooks into component status notifications, and writes information about
-- component instances to this table.
CREATE TABLE registered_component (

  -- The type of the component (e.g. topology-processor).
  component_type            VARCHAR(255)       NOT NULL,

  -- The instance id of the component (e.g. topology-processor-bs9resn).
  instance_id               VARCHAR(255)       NOT NULL,

  -- The JVM id of this particular incarnation of the instance.
  jvm_id                    BIGINT             NOT NULL,

  -- The IP address the component is reachable at.
  ip_address                VARCHAR(255)       NOT NULL,

  -- The port the component is reachable at.
  port                      INT                NOT NULL,

  -- The route to prefix all component HTTP paths with.
  route                     VARCHAR(255)       NOT NULL,

  -- Additional information about the component that may be helpful for debugging.
  -- This could include things like version numbers, configuration property values, and so on.
  auxiliary_info            TEXT               DEFAULT NULL,

  -- The time the component first registered.
  registration_time         TIMESTAMP          NOT NULL,

  -- The last time this component's entry was updated.
  last_update_time          TIMESTAMP          NOT NULL,

  -- The current health status of the component.
  status                    INT                NOT NULL,

  -- Description of the current health status.
  status_description        TEXT               DEFAULT NULL,

  -- The time of the last status change.
  -- The difference between this value and the current time is how long the component has been
  -- at its current health status.
  last_status_change_time   TIMESTAMP          NOT NULL,

  PRIMARY KEY (instance_id)
);