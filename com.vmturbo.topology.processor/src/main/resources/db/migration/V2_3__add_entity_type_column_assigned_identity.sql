-- Add a column for the storing the type of the entity.

-- Store the protobuf Enum numeric value of the CommodityDTO.EntityType
-- instead of string as it is more space efficient.
-- We store a default MAX_SIGNED_VALUE for the existing rows inorder
-- to identify if the existing column has been migrated to the proper type or not.
-- The protobuf EnumType is represented by int in java.
ALTER TABLE assigned_identity ADD entity_type INT NOT NULL DEFAULT 2147483647;
