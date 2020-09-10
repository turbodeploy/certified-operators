package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;

/**
 * A filter of entity demand based on the entity state (suspended or terminated).
 */
public class EntityStateFilter {

    private final boolean includeSuspendedEntities;

    private final boolean includeTerminatedEntities;


    private EntityStateFilter(boolean includeSuspendedEntities,
                              boolean includeTerminatedEntities) {
        this.includeSuspendedEntities = includeSuspendedEntities;
        this.includeTerminatedEntities = includeTerminatedEntities;
    }

    /**
     * Filters demand, based on the configuration of this filter (whether suspended and/or terminated
     * demand should be allowed to pass the filter) and the state the entity.
     * @param entityAggregate The entity demand aggregate to filter.
     * @return True, if the entity demand aggregate passes the filter. False, otherwise.
     */
    public boolean filterDemand(@Nonnull ClassifiedEntityDemandAggregate entityAggregate) {
        return (includeSuspendedEntities || !entityAggregate.isSuspended())
                && (includeTerminatedEntities || !entityAggregate.isTerminated());
    }

    /**
     * A factory class for creating {@link EntityStateFilter} instances.
     */
    public static class EntityStateFilterFactory {

        /**
         * Constructs a new {@link EntityStateFilter} instance.
         * @param includeSuspendedEntities IF true, suspended entities will pass the filter.
         * @param includeTerminatedEntities IF true, terminated entities will pass the filter.
         * @return The newly constructed {@link EntityStateFilter} instance.
         */
        @Nonnull
        public EntityStateFilter newFilter(boolean includeSuspendedEntities,
                                           boolean includeTerminatedEntities) {
            return new EntityStateFilter(
                    includeSuspendedEntities,
                    includeTerminatedEntities);
        }
    }
}
