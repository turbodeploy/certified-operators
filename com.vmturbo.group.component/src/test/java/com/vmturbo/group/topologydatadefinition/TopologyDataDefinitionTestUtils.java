package com.vmturbo.group.topologydatadefinition;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition.TagBasedGenerationAndConnection;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.DynamicConnectionFilters;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition.AssociatedEntitySelectionCriteria;
import com.vmturbo.common.protobuf.search.Search.SearchParameters.FilterSpecs;
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
     * @param connectedEntityType The EntityType representing the type of entity in the group.
     * @return TopologyDataDefinition based on the parameters.
     */
    public static TopologyDataDefinition createManualTopologyDataDefinition(
            @Nonnull EntityType entityType, @Nonnull String name, long groupId,
            @Nonnull EntityType connectedEntityType) {
        return TopologyDataDefinition.newBuilder()
                .setManualEntityDefinition(ManualEntityDefinition.newBuilder()
                        .setEntityType(entityType)
                        .setEntityName(name)
                        .addAssociatedEntities(AssociatedEntitySelectionCriteria.newBuilder()
                                .setAssociatedGroup(GroupID.newBuilder()
                                        .setId(groupId)
                                        .build())
                                .setConnectedEntityType(connectedEntityType)
                                .build())
                        .build())
                .build();
    }

    /**
     * Create a manual topology data definition with a static list of associated entities.
     *
     * @param entityType EntityType to create.
     * @param name Name of created entity.
     * @param associatedEntityType EntityType of associated entities.
     * @param associatedIds Set of Longs giving ids of associated entities.
     * @return TopologyDataDefinition representing the manual topology data definition.
     */
    public static TopologyDataDefinition createManualTopologyDataDefinition(
            @Nonnull EntityType entityType, @Nonnull String name,
            @Nonnull EntityType associatedEntityType, @Nonnull Set<Long> associatedIds) {
        return TopologyDataDefinition.newBuilder()
                .setManualEntityDefinition(ManualEntityDefinition.newBuilder()
                        .setEntityType(entityType)
                        .setEntityName(name)
                        .addAssociatedEntities(AssociatedEntitySelectionCriteria.newBuilder()
                                .setStaticAssociatedEntities(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType.newBuilder()
                                                .setType(MemberType.newBuilder()
                                                        .setEntity(associatedEntityType.getNumber())
                                                        .build())
                                                .addAllMembers(associatedIds)
                                                .build())
                                        .build())
                                .setConnectedEntityType(associatedEntityType)
                                .build())
                        .build())
                .build();
    }

    /**
     * Create a manual topology data definition with entity filters defininging associated entities.
     *
     * @param entityType The type of entity to create.
     * @param name The name of the created entity.
     * @param associatedEntityType The type of associated entities.
     * @return a manual topology data definition with entity filters defining associated entities.
     */
    public static TopologyDataDefinition createManualTopologyDataDefinition(
            @Nonnull EntityType entityType, @Nonnull String name,
            @Nonnull EntityType associatedEntityType) {
        return TopologyDataDefinition.newBuilder().setManualEntityDefinition(
                ManualEntityDefinition.newBuilder()
                        .setEntityType(entityType)
                        .setEntityName(name)
                        .addAssociatedEntities(AssociatedEntitySelectionCriteria.newBuilder()
                                .setDynamicConnectionFilters(DynamicConnectionFilters.newBuilder()
                                        .addEntityFilters(FilterSpecs.newBuilder()
                                                .setExpressionType("REGX")
                                                .setExpressionValue(".*")
                                                .setFilterType("foo")
                                                .setIsCaseSensitive(false)
                                                .build())
                                        .build())
                                .setConnectedEntityType(associatedEntityType)
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
