package com.vmturbo.common.protobuf;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Utilities for dealing with protobuf messages in topology/TopologyDTO.proto.
 */
public final class TopologyDTOUtil {

    private TopologyDTOUtil() {
    }

    /**
     * Determine whether or not an entity is placed in whatever topology it belongs to.
     *
     * @param entity The {@link TopologyDTO.TopologyEntityDTO} to evaluate.
     * @return Whether or not the entity is placed (in whatever topology it belongs to).
     */
    public static boolean isPlaced(@Nonnull final TopologyDTO.TopologyEntityDTO entity) {
        return entity.getCommoditiesBoughtFromProvidersList().stream()
                // Only non-negative numbers are valid IDs, so we only consider an entity
                // to be placed if all commodities are bought from valid provider IDs.
                .allMatch(commBought -> commBought.hasProviderId() && commBought.getProviderId() >= 0);
    }

    /**
     * Determine whether or not the topology described by a {@link TopologyDTO.TopologyInfo}
     * is generated for a plan.
     *
     * @param topologyInfo The {@link TopologyDTO.TopologyInfo} describing a topology.
     * @return Whether or not the described topology is generated for a plan.
     */
    public static boolean isPlan(@Nonnull final TopologyDTO.TopologyInfo topologyInfo) {
        return topologyInfo.hasPlanInfo();
    }

    /**
     * Determine whether or not the topology described by a {@link TopologyDTO.TopologyInfo}
     * is generated for a plan of the given type.
     *
     * @param type A type of plan project.
     * @param topologyInfo The {@link TopologyDTO.TopologyInfo} describing a topology.
     * @return Whether or not the described topology is generated for a plan of the given type.
     */
    public static boolean isPlanType(@Nonnull final PlanProjectType type,
                                     @Nonnull final TopologyDTO.TopologyInfo topologyInfo) {
        return isPlan(topologyInfo) && topologyInfo.getPlanInfo().getPlanType() == type;
    }

    /**
     * Create a mapping from entityId to its type.
     *
     * @param entityDTOs Set of TopologyDTOs
     * @return Mapping from the entityId to its type.
     */
    public static Map<Long, Integer> getEntityIdToEntityTypeMapping(@Nonnull Set<TopologyEntityDTO> entityDTOs) {
            return entityDTOs.stream()
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, TopologyEntityDTO::getEntityType));
    }
}
