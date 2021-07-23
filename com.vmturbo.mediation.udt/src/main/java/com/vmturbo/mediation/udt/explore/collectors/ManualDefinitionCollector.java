package com.vmturbo.mediation.udt.explore.collectors;

import static com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import static com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.DynamicConnectionFilters;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition.AssociatedEntitySelectionCriteria;
import com.vmturbo.mediation.udt.explore.DataProvider;
import com.vmturbo.mediation.udt.inventory.UdtChildEntity;
import com.vmturbo.mediation.udt.inventory.UdtEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The collector for manual topology data definition.
 */
public class ManualDefinitionCollector extends UdtCollector<ManualEntityDefinition> {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Constructor.
     *
     * @param definitionId - ID of topology data definition.
     * @param definition   - topology data definition instance.
     */
    @ParametersAreNonnullByDefault
    public ManualDefinitionCollector(Long definitionId, ManualEntityDefinition definition) {
        super(definitionId, definition);
    }

    /**
     * A method for populating UDT entities.
     *
     * @param dataProvider - an instance of {@link DataProvider}.
     * @return a set of UDT entities.
     */
    @Nonnull
    @Override
    public Set<UdtEntity> collectEntities(@Nonnull DataProvider dataProvider) {
        if (!isValidDefinition()) {
            LOGGER.error("Topology definition is incorrect: {}", definition);
            return Collections.emptySet();
        }
        Set<UdtChildEntity> children = definition.getAssociatedEntitiesList().stream()
                .map(criteria -> getChildEntities(criteria, dataProvider))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        final UdtEntity udtEntity = new UdtEntity(definition.getEntityType(),
                String.valueOf(definitionId), definition.getEntityName(), children);
        return Collections.singleton(udtEntity);
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private Set<UdtChildEntity> getChildEntities(AssociatedEntitySelectionCriteria criteria, DataProvider dataProvider) {
        if (criteria.hasConnectedEntityType()) {
            final EntityType entityType = criteria.getConnectedEntityType();
            switch (criteria.getSelectionTypeCase()) {
                case STATIC_ASSOCIATED_ENTITIES:
                    return getStaticChildEntities(entityType, criteria.getStaticAssociatedEntities());
                case ASSOCIATED_GROUP:
                    return getGroupChildEntities(entityType, criteria.getAssociatedGroup(), dataProvider);
                case DYNAMIC_CONNECTION_FILTERS:
                    return getFilteredChildEntities(entityType, criteria.getDynamicConnectionFilters(), dataProvider);
                default:
                    LOGGER.warn("Manual UDT: unexpected selection type case: {}", criteria.getSelectionTypeCase());
                    return Collections.emptySet();
            }
        }
        LOGGER.warn("Manual UDT: criteria has no connected entity type. Skip criteria.");
        return Collections.emptySet();
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private Set<UdtChildEntity> getStaticChildEntities(EntityType entityType, StaticMembers staticMembers) {
        Set<UdtChildEntity> children = Sets.newHashSet();
        for (StaticMembers.StaticMembersByType memberByType : staticMembers.getMembersByTypeList()) {
            memberByType.getMembersList().forEach(memberId ->
                    children.add(new UdtChildEntity(memberId, entityType))
            );
        }
        return children;
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private Set<UdtChildEntity> getGroupChildEntities(EntityType entityType, GroupID groupId,
                                                      DataProvider dataProvider) {
        final Set<Long> membersOids = dataProvider.getGroupMembersIds(groupId);
        return membersOids.stream().map(oid -> new UdtChildEntity(oid, entityType))
                .collect(Collectors.toSet());
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private Set<UdtChildEntity> getFilteredChildEntities(EntityType entityType,
                                                         DynamicConnectionFilters filters,
                                                         DataProvider dataProvider) {
        return dataProvider.searchEntities(filters.getSearchParametersList()).stream()
                .map(entity -> new UdtChildEntity(entity.getOid(), entityType))
                .collect(Collectors.toSet());
    }

    private boolean isValidDefinition() {
        return definition.hasEntityName() && definition.hasEntityType()
                && !definition.getAssociatedEntitiesList().isEmpty();
    }

}
