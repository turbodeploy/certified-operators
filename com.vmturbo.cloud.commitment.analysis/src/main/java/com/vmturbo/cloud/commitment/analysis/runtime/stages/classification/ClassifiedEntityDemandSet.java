package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

/**
 * Contains classified entity cloud tier demand (demand indexed by the associated entity). This demand
 * set is the output of {@link DemandClassificationStage}.
 */
@Immutable
public interface ClassifiedEntityDemandSet {

    /**
     * The allocated classified entity demand.
     * @return The allocated classified entity demand.
     */
    @Nonnull
    Set<ClassifiedEntityDemandAggregate> classifiedAllocatedDemand();

    /**
     * The projected classified entity demand.
     * @return The projected classified entity demand.
     */
    @Nonnull
    Set<ClassifiedEntityDemandAggregate> classifiedProjectedDemand();
}
