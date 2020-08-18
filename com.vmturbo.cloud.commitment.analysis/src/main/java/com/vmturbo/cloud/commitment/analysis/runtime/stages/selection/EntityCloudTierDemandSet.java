package com.vmturbo.cloud.commitment.analysis.runtime.stages.selection;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;

/**
 * The set of selected demand as the output of demand selection.
 */
@Immutable
public interface EntityCloudTierDemandSet {

    /**
     * The set of allocated demand mappings.
     * @return An immutable set of allocated demand mappings.
     */
    @Nonnull
    Set<EntityCloudTierMapping> allocatedDemand();

    /**
     * The set of projected demand mappings.
     * @return An immutable set of projected demand mappings.
     */
    @Nonnull
    Set<EntityCloudTierMapping> projectedDemand();

}
