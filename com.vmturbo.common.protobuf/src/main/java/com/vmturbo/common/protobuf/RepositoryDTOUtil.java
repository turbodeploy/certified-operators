package com.vmturbo.common.protobuf;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Utilities for working with messages defined in "repository/RepositoryDTO.proto".
 */
public class RepositoryDTOUtil {

    /**
     * Compare a {@link TopologyEntityDTO} against this reader's {@link TopologyEntityFilter}.
     *
     * @param entity The entity to compare.
     * @return Whether or not the entity matches the filter.
     */
    public static boolean entityMatchesFilter(@Nonnull final TopologyEntityDTO entity,
                                              @Nonnull final TopologyEntityFilter filter) {
        // If the filter wants only unplaced entities, and this entity is placed, the entity
        // doesn't match.
        return !(filter.getUnplacedOnly() && TopologyDTOUtil.isPlaced(entity));
    }

    /**
     * Get the complete list of OIDs in a {@link SupplyChainNode}, regardless of state.
     *
     * @param supplyChainNode The {@link SupplyChainNode}.
     * @return A set of OIDs of all entities that are members of that node.
     */
    @Nonnull
    public static Set<Long> getAllMemberOids(@Nonnull final SupplyChainNode supplyChainNode) {
        return supplyChainNode.getMembersByStateMap().values().stream()
                .map(MemberList::getMemberOidsList)
                .flatMap(List::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Get the number of OIDs in a {@link SupplyChainNode}, regardless of state.
     *
     * @param supplyChainNode The {@link SupplyChainNode}.
     * @return The number of entities that are members of that node.
     */
    public static int getMemberCount(@Nonnull final SupplyChainNode supplyChainNode) {
        return supplyChainNode.getMembersByStateMap().values().stream()
                .map(MemberList::getMemberOidsCount)
                .mapToInt(Integer::valueOf)
                .sum();
    }
}
