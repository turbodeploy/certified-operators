package com.vmturbo.cost.component.cloud.commitment.coverage;

import java.util.List;

import org.immutables.gson.Gson;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Combine topology info with the topology's cloud commitment coverage.
 */
@HiddenImmutableImplementation
@Immutable
@Gson.TypeAdapters
public interface CoverageInfo {

    /**
     * Gets the {@link TopologyInfo}.
     *
     * @return the {@link TopologyInfo}.
     */
    TopologyInfo topologyInfo();

    /**
     * Gets the list of {@link CloudCommitmentCoverage}.
     *
     * @return the list of {@link CloudCommitmentCoverage}.
     */
    List<CloudCommitmentCoverage> coverageDataPoints();

    /**
     * Constructs and returns a new {@link CoverageInfo.Builder} instance.
     *
     * @return The newly constructed {@link CoverageInfo.Builder} instance.
     */
    static CoverageInfo.Builder builder() {
        return new CoverageInfo.Builder();
    }

    /**
     * A builder class for constructing {@link CoverageInfo} instances.
     */
    class Builder extends ImmutableCoverageInfo.Builder {}
}
