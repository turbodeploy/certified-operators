package com.vmturbo.cost.component.cloud.commitment.mapping;

import java.util.List;

import org.immutables.gson.Gson;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Combine topology info with the topology's cloud commitment mappings.
 */
@HiddenImmutableImplementation
@Immutable
@Gson.TypeAdapters
public interface MappingInfo {

    /**
     * Gets the {@link TopologyInfo}.
     *
     * @return the {@link TopologyInfo}.
     */
    TopologyInfo topologyInfo();

    /**
     * Gets the list of {@link CloudCommitmentMapping}.
     *
     * @return the list of {@link CloudCommitmentMapping}.
     */
    List<CloudCommitmentMapping> cloudCommitmentMappings();

    /**
     * Constructs and returns a new {@link MappingInfo.Builder} instance.
     *
     * @return The newly constructed {@link MappingInfo.Builder} instance.
     */
    static MappingInfo.Builder builder() {
        return new MappingInfo.Builder();
    }

    /**
     * A builder class for constructing {@link MappingInfo} instances.
     */
    class Builder extends ImmutableMappingInfo.Builder {}
}
