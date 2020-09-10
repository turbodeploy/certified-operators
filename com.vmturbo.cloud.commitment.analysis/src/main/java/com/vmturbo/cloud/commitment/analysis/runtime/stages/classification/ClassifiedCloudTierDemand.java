package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;

/**
 * Classified cloud tier demand, without a relationship to an entity.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface ClassifiedCloudTierDemand {

    /**
     * An empty {@link ClassifiedCloudTierDemand} instance.
     */
    ClassifiedCloudTierDemand EMPTY_CLASSIFICATION = ClassifiedCloudTierDemand.builder().build();

    /**
     * The classified demand, indexed by the demand classification.
     * @return The classified entity demand.
     */
    @Nonnull
    Map<DemandClassification, Set<DemandTimeSeries>> classifiedDemand();

    /**
     * The allocated cloud tier demand.
     * @return The allocated cloud tier demand.
     */
    @Nonnull
    Optional<DemandTimeSeries> allocatedDemand();

    /**
     * Checks whether this instance contains any classified demand.
     * @return True, if this demand does not contain any classified demand.
     */
    @Derived
    default boolean isEmpty() {
        return classifiedDemand().isEmpty();
    }

    /**
     * Constructs and returns a new builder instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for creating {@link ClassifiedCloudTierDemand} instances.
     */
    class Builder extends ImmutableClassifiedCloudTierDemand.Builder {}
}
