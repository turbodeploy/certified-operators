package com.vmturbo.common.protobuf;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
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
}
