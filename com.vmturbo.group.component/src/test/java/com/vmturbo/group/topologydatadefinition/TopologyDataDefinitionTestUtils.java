package com.vmturbo.group.topologydatadefinition;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition.TagBasedGenerationAndConnection;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition.AssociatedEntitySelectionCriteria;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility class for testing TopologyDataDefinition related stuff.
 */
public class TopologyDataDefinitionTestUtils {

    /**
     * Private constructor.
     */
    private TopologyDataDefinitionTestUtils() {}

    /**
     * Create a basic manual TopologyDataDefinition.
     *
     * @param entityType The type of entity the definition will create.
     * @param name The name of the entity the definition will create.
     * @param groupId A groupId to use for the associated group field.
     * @return TopologyDataDefinition based on the parameters.
     */
    public static TopologyDataDefinition createManualTopologyDataDefinition(
            final EntityType entityType, final String name, long groupId) {
        return TopologyDataDefinition.newBuilder()
                .setManualEntityDefinition(ManualEntityDefinition.newBuilder()
                        .setEntityType(entityType)
                        .setEntityName(name)
                        .addAssociatedEntities(AssociatedEntitySelectionCriteria.newBuilder()
                                .setAssociatedGroup(GroupID.newBuilder()
                                        .setId(groupId)
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /**
     * Create an automated TopologyDataDefinition.
     *
     * @param entityType EntityType to be created by the definition.
     * @param prefix Prefix for naming new entities that are created.
     * @param connectedEntityType Type of entity to use for applying tag key.
     * @param tagKey Tag key value.
     * @return automated TopologyDataDefinition based on parameters.
     */
    public static TopologyDataDefinition createAutomatedTopologyDataDefinition(
        @Nonnull EntityType entityType,
        @Nonnull final String prefix,
        @Nonnull EntityType connectedEntityType,
        @Nonnull final String tagKey) {
        return TopologyDataDefinition.newBuilder()
            .setAutomatedEntityDefinition(AutomatedEntityDefinition.newBuilder()
                .setEntityType(entityType)
                .setNamingPrefix(prefix)
                .setConnectedEntityType(connectedEntityType)
                .setTagGrouping(TagBasedGenerationAndConnection.newBuilder()
                    .setTagKey(tagKey)
                    .build())
                .build())
            .build();
    }
}
