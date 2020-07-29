package com.vmturbo.market.topology.conversions;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class to represent collapsed relationship for trader creation.
 */
public class CollapsedTraderHelper {

    private CollapsedTraderHelper() {}

    /**
     * Mapping from entity type to collapsed entity type.
     * Map recording entity and its collapsed entity type(direct provider type).
     */
    private static final Map<Integer, Integer> ENTITY_COLLAPSEDENTITY_TYPE_MAP =
            ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.VIRTUAL_VOLUME_VALUE);

    /**
     * Mapping from entity type to new provider type after collapsing.
     * Map recording entity type to provider type for collapsed entity.
     */
    private static final Map<Integer, Integer> ENTITY_NEWPROVIDER_TYPE_MAP =
            ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.STORAGE_TIER_VALUE);

    /**
     * Get new provider type after collapsing. Return null if no need to collapse.
     *
     * @param entityType trader entity type.
     * @param directProviderType direct provider type to be collapsed.
     * @return final provider type if direct provider can be collapsed.
     *         return null if no need to collapse.
     */
    @Nullable
    public static Integer getNewProviderTypeAfterCollapsing(Integer entityType, Integer directProviderType) {
        return shouldDirectProviderCollapse(entityType, directProviderType) ? ENTITY_NEWPROVIDER_TYPE_MAP.get(entityType) : null;
    }

    /**
     * Check if direct provider should be collapsed.
     *
     * @param entityType trader entity type.
     * @param directProviderType direct provider type.
     * @return true if direct provider should be collapsed.
     */
    private static boolean shouldDirectProviderCollapse(Integer entityType, Integer directProviderType) {
        return directProviderType == null ? false
                : directProviderType.equals(ENTITY_COLLAPSEDENTITY_TYPE_MAP.get(entityType));
    }

    /**
     * Get collapsed entity type.
     *
     * @param entityType trader entity type.
     * @return collapsed entity type if exists.
     */
    @Nullable
    public static Integer getCollapsedEntityType(Integer entityType) {
        return ENTITY_COLLAPSEDENTITY_TYPE_MAP.get(entityType);
    }
}
