package com.vmturbo.repository.plan.db;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.components.api.FormattedString;

/**
 * Exception thrown when a particular plan-related topology is not found in the
 * {@link PlanEntityStore}.
 */
public class TopologyNotFoundException extends Exception {
    /**
     * Create a new exception.
     *
     * @param topologyId The ID of the topology that was not found.
     */
    public TopologyNotFoundException(final long topologyId) {
        super(FormattedString.format("Topology with ID {} not found in database.", topologyId));
    }

    /**
     * Create a new exception.
     *
     * @param planId The id of the plan.
     * @param topologyType The id of the topology.
     */
    public TopologyNotFoundException(long planId, TopologyType topologyType) {
        super(FormattedString.format("{} topology not found for context {}",
                topologyType, planId));
    }
}
