package com.vmturbo.cloud.commitment.analysis.spec;

import javax.annotation.Nonnull;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * A wrapping class of a cloud commitment spec, containing the spec and additional data.
 * @param <SpecTypeT> The cloud commitment spec type.
 */
@Immutable
public interface CloudCommitmentSpecData<SpecTypeT> {

    /**
     * The wrapped cloud commitment spec.
     * @return The wrapped cloud commitment spec.
     */
    @Nonnull
    SpecTypeT spec();

    /**
     * The spec ID.
     * @return The spec ID.
     */
    long specId();

    /**
     * The cloud tier associated with {@link #spec()}. This value is not used
     * in equality or hashing checks. This may be a directly reference tier of the spec or a
     * representative tier within a family.
     *
     * @return The cloud tier associated with the spec.
     */
    @Value.Auxiliary
    TopologyEntityDTO cloudTier();
}
