package com.vmturbo.common.protobuf;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;

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
        return !(filter.getUnplacedOnly() && TopologyDTOUtil.isPlaced(entity))
            // Entity is valid if filter has no entity type or it contains the entity type
            // of given entity.
            && (filter.getEntityTypesList().isEmpty() ||
                filter.getEntityTypesList().contains(entity.getEntityType()));
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


    /**
     * Utility function for creating a stream of topology entity dto's from an entity batch iterator.
     * @param batchIterator
     * @return
     */
    public static Stream<PartialEntity> topologyEntityStream(Iterator<PartialEntityBatch> batchIterator) {

        Iterable<PartialEntityBatch> batchIterable = () -> batchIterator;
        return StreamSupport.stream(batchIterable.spliterator(), false)
                .flatMap(entityBatch -> entityBatch.getEntitiesList().stream());
    }

}
