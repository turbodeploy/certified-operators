package com.vmturbo.reserved.instance.coverage.allocator.matcher;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentScope;

/**
 * A base configuration specifying how to generate a coverage key for cloud commitments.
 */
@HiddenImmutableImplementation
@Immutable
public interface CommitmentMatcherConfig {

    /**
     * Indicates how to match a cloud commitment based on scope.
     * @return The {@link CloudCommitmentScope}, indicating how to match a cloud commitment to
     * coverage entities based on scope.
     */
    @Nonnull
    CloudCommitmentScope scope();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link CommitmentMatcherConfig} instances.
     */
    class Builder extends ImmutableCommitmentMatcherConfig.Builder {}
}
