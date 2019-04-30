--
-- This migration is part of moving group definitions from ArangoDB to SQL.
--
-- A group (using "grouping" because group is a reserved keyword in MySQL) is a set of entities
-- that can be treated collectively for the purpose of settings, policies, statistics,
-- and other operations.
CREATE TABLE grouping (
  -- The ID assigned to the group. The id is required to be unique.
  id                   BIGINT        NOT NULL UNIQUE,

  -- The name of the group. The name is required to be unique.
  name                 VARCHAR(255)  NOT NULL UNIQUE,

  -- The origin of the group.
  -- This is the integer representation of Group.Origin.
  origin               INT        NOT NULL,

  -- The type of the group.
  -- This is the integer representation of Group.Type.
  type                 INT        NOT NULL,

  -- The ID of the target that discovered the group.
  -- Only set if the group was discovered by a target.
  discovered_by_id     BIGINT,

  -- The serialized protobuf object containing detailed information about
  -- the group. This should be either a "GroupInfo", "ClusterInfo", or "TempGroupInfo" depending
  -- on the type of group. (see Group.proto).
  group_data           BLOB          NOT NULL,

  PRIMARY KEY(id)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

-- A policy outlines additional constraints the market should respect - for example, restricting
-- a group of VMs to only run on a particular cluster. See the Policy protobuf definition.
CREATE TABLE policy (
  -- The ID assigned to the policy. The id is required to be unique.
  id                   BIGINT        NOT NULL UNIQUE,

  -- The name of the group. The name is required to be unique.
  name                 VARCHAR(255)  NOT NULL UNIQUE,

  -- A bit representing whether or not the policy is enabled.
  enabled              BIT(1)        NOT NULL,

  -- The ID of the target that discovered the policy.
  -- Only set if the policy was discovered by a target.
  discovered_by_id     BIGINT,

  -- The serialized protobuf object containing detailed information about
  -- the policy. This should be a "PolicyInfo" (see PolicyDTO.proto).
  policy_data          BLOB          NOT NULL,

  PRIMARY KEY(id)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

-- A join table to keep track of which policies reference which groups.
-- The application is responsible for keeping this up-to-date when new policies
-- are created, and when policies are updated to reference different groups.
CREATE TABLE policy_group (
  -- The ID of the policy referencing the group.
  policy_id            BIGINT         NOT NULL,

  -- The ID of the group being referenced.
  group_id             BIGINT         NOT NULL,

  PRIMARY KEY(group_id, policy_id),

  CONSTRAINT fk_group_id
    FOREIGN KEY(group_id) REFERENCES grouping(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT fk_policy_id
    FOREIGN KEY(policy_id) REFERENCES policy(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE
)
