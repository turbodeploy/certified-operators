package com.vmturbo.market.topology.conversions;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class to represent collapsed relationship for trader creation.
 */
public class CollapsedTraderHelper {

    /**
     * Collapsing entity topology table.
     * Table recording entity and its direct provider(collapsed entity), to second layer provider(provider for collapsed entity).
     */
    private static final Table<Integer, Integer, Integer> ENTITY_COLLAPSEDENTITY_PROVIDER_TYPE_TABLE =
            ImmutableTable.of(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.VIRTUAL_VOLUME_VALUE, EntityType.STORAGE_TIER_VALUE);
    /**
     * Collapsing entity inverse table.
     * Table recording entity and second layer provider(provider for collapsed entity), to collapsed entity.
     */
    private static final Table<Integer, Integer, Integer> ENTITY_PROVIDER_COLLAPSEDENTITY_TYPE_TABLE =
            ImmutableTable.of(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.STORAGE_TIER_VALUE, EntityType.VIRTUAL_VOLUME_VALUE);

    /**
     * Get final provider type after collapsing.
     *
     * @param entityType trader entity type.
     * @param directProviderType direct provider type to be collapsed.
     * @return final provider type if direct provider can be collapsed.
     */
    @Nullable
    public static Integer getProviderTypeAfterCollapsing(Integer entityType, Integer directProviderType) {
        return ENTITY_COLLAPSEDENTITY_PROVIDER_TYPE_TABLE.get(entityType, directProviderType);
    }

    /**
     * Get collapsed entity type if exists.
     *
     * @param entityType trader entity type.
     * @param providerType trader final provider type.
     * @return collapsed entity type if exists.
     */
    @Nullable
    public static Integer getCollapsedEntityType(Integer entityType, Integer providerType) {
        return ENTITY_PROVIDER_COLLAPSEDENTITY_TYPE_TABLE.get(entityType, providerType);
    }
}
