package com.vmturbo.cost.component.cloud.commitment.coverage;

import java.util.Map;

import javax.annotation.Nonnull;

import org.immutables.gson.Gson;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentCoverage;
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
    @Nonnull
    TopologyInfo topologyInfo();

    /**
     * Gets the map of entity ID to {@link ScopedCommitmentCoverage}.
     *
     * @return the map of entity ID to {@link ScopedCommitmentCoverage}.
     */
    @Nonnull
    Map<Long, ScopedCommitmentCoverage> entityCoverageMap();

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
    @Nonnull
    class Builder extends ImmutableCoverageInfo.Builder {}
}
