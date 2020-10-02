package com.vmturbo.reserved.instance.coverage.allocator.matcher.entity;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentLocation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentScope;

/**
 * A configuration specifying how to generate coverage keys representing entities (e.g. VMs, DBs, etc).
 */
public interface EntityMatcherConfig {

    /**
     * The set of {@link CloudCommitmentLocation} instances, representing how an entity's location should
     * be matched to a cloud commitment through a coverage key.
     * @return The set of {@link CloudCommitmentLocation} instances.
     */
    @Nonnull
    Set<CloudCommitmentLocation> locations();

    /**
     * The set of {@link CloudCommitmentScope} instances, representing how an entity's scope should
     * be matched to a cloud commitment through a coverage key.
     * @return The set of {@link CloudCommitmentScope} instances.
     */
    @Nonnull
    Set<CloudCommitmentScope> scopes();
}
