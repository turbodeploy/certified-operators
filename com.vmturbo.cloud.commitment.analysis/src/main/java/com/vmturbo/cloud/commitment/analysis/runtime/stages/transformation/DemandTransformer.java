package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;

/**
 * An interface for transforming {@link ClassifiedEntityDemandAggregate} instances.
 */
@FunctionalInterface
public interface DemandTransformer {

    /**
     * Transforms the provided demand contained within {@code entityAggregate}.
     * @param entityAggregate The entity aggregate to transform.
     * @return The transformed entity aggregate.
     */
    @Nonnull
    ClassifiedEntityDemandAggregate transformDemand(@Nonnull ClassifiedEntityDemandAggregate entityAggregate);

    /**
     * Merges two {@link DemandTransformer} instances into a single transformer.
     * @param secondTransformer The transformer to merge with "this" transformer.
     * @return A single transformation representing "this" and {@code secondTransformer} executed
     * consecutively.
     */
    default DemandTransformer andThen(@Nonnull DemandTransformer secondTransformer) {
        return (entityAggregate) -> secondTransformer.transformDemand(transformDemand(entityAggregate));
    }
}
