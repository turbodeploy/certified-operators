package com.vmturbo.repository.plan.db;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;

/**
 * Utility class to represent the plan ID and the topology type (source or projected) within
 * that plan.
 */
public class TopologySelection {
    private final long planId;
    private final long topologyId;
    private final TopologyType topologyType;

    /**
     * Create a new {@link TopologySelection}.
     * Package-level visibility because this should be created in the {@link PlanEntityStore}
     * implementation, not by callers.
     *
     * @param planId The id of the plan.
     * @param topologyType The topology type.
     * @param topologyId The id of the topology.
     */
    TopologySelection(final long planId, final TopologyType topologyType, final long topologyId) {
        this.planId = planId;
        this.topologyType = topologyType;
        this.topologyId = topologyId;
    }

    public long getTopologyId() {
        return topologyId;
    }

    public long getPlanId() {
        return planId;
    }

    @Nonnull
    public TopologyType getTopologyType() {
        return topologyType;
    }
}
