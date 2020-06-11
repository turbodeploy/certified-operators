package com.vmturbo.topology.processor.topology;

import static com.vmturbo.common.protobuf.utils.StringConstants.ALLOCATION_PLAN_KEEP_STORAGE_TIERS;
import static com.vmturbo.common.protobuf.utils.StringConstants.CONSUMPTION_PLAN_SKIP_STORAGE_TIERS;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATACENTER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DISK_ARRAY;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_TIER;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Utility class with some helper functions used during Cloud migration plan.
 */
public class CloudMigrationPlanUtils {
    /**
     * Private constructor.
     */
    private CloudMigrationPlanUtils() {}

    /**
     * Whether to include the cloud storage tier in the destination set of placement policy.
     * Certain storage tiers need to be skipped from cloud migration plans.
     * For Lift_n_Shift: Skip all except GP2 and ManagedPremium.
     * For Optimized: Skip HDD and SSD.
     *
     * @param cloudStorageTier Cloud storage tier entity.
     * @param topologyInfo TopologyInfo having info about plan sub type.
     * @return True if this storage tier needs to be included, e.g GP2 for Allocation plan.
     */
    public static boolean includeCloudStorageTier(@Nonnull final TopologyEntity cloudStorageTier,
                                                  @Nonnull final TopologyInfo topologyInfo) {
        // Don't see another way to get storage type, other than looking at display name.
        String storageType = cloudStorageTier.getTopologyEntityDtoBuilder().getDisplayName();
        if (TopologyDTOUtil.isResizableCloudMigrationPlan(topologyInfo)) {
            // Optimized plan (with resize enabled). We are skipping HDD/SSD etc, so
            // need to return false in those cases.
            return !CONSUMPTION_PLAN_SKIP_STORAGE_TIERS.contains(storageType);
        }
        return ALLOCATION_PLAN_KEEP_STORAGE_TIERS.contains(storageType);
    }

    /**
     * Gets the providers for the source migration entities that we need for the plan.
     * Only needed providers are returned, instead of everything under the entities, as that
     * ends up returning providers (e.g Storage) that don't have any direct connection to the
     * workloads being migrated.
     *
     * @param migrationWorkloads Set of workloads being migrated.
     * @return Map keyed off of provider entity type, with value being a set of those entities.
     */
    @Nonnull
    public static Map<EntityType, Set<TopologyEntity>> getProviders(
            @Nonnull final Set<TopologyEntity> migrationWorkloads) {
        Map<EntityType, Set<TopologyEntity>> providerByType = new HashMap<>();
        migrationWorkloads
                .stream()
                .map(TopologyEntity::getProviders)
                .flatMap(Collection::stream)
                .forEach(provider -> {
                    EntityType providerType = EntityType.forNumber(provider.getEntityType());
                    switch (providerType) {
                        case PHYSICAL_MACHINE:
                        case STORAGE:
                        case VIRTUAL_VOLUME:
                            // For these, we need to add the parent providers as well.
                            for (TopologyEntity parent : provider.getProviders()) {
                                EntityType parentType = EntityType.forNumber(parent.getEntityType());
                                if (parentType == DATACENTER
                                        || parentType == DISK_ARRAY
                                        || parentType == STORAGE_TIER) {
                                    providerByType.computeIfAbsent(parentType, k -> new HashSet<>())
                                            .add(parent);
                                }
                            }
                            break;
                    }
                    // Add the current provider.
                    providerByType.computeIfAbsent(providerType, k -> new HashSet<>())
                            .add(provider);
                });
        return providerByType;
    }
}
