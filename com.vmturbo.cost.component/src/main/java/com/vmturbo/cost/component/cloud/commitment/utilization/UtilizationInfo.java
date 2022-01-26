package com.vmturbo.cost.component.cloud.commitment.utilization;

import java.util.Map;

import org.immutables.gson.Gson;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentUtilization;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Utilization statistics.
 */
@HiddenImmutableImplementation
@Immutable
@Gson.TypeAdapters
public interface UtilizationInfo {

    /**
     * Gets the {@link TopologyInfo}.
     *
     * @return the {@link TopologyInfo}.
     */
    TopologyInfo topologyInfo();

    /**
     * Gets the mapping between commitment ID and utilization.
     *
     * @return the mapping between commitment ID and utilization.
     */
    Map<Long, ScopedCommitmentUtilization> commitmentUtilizationMap();

    /**
     * Converts this {@link UtilizationInfo} instance to a {@link Builder}.
     * @return The new {@link Builder} instance.
     */
    default Builder toBuilder() {
        return UtilizationInfo.builder().from(this);
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     *
     * @return The newly constructed {@link Builder} instance.
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link UtilizationInfo} instances.
     */
    class Builder extends ImmutableUtilizationInfo.Builder {}
}
