package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cloud.commitment.analysis.demand.CloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;

/**
 * Represents all {@link CloudTierDemand} associated with a single entity, aggregated and classified.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface ClassifiedEntityDemandAggregate {

    /**
     * The entity OID of the mapping.
     * @return The entity OID.
     */
    long entityOid();

    /**
     * The OID of the account containing the entity.
     * @return The OID of the account containing the entity.
     */
    long accountOid();

    /**
     * The billing family ID of the entity.
     * @return The ID of the billing family for the entity. May be empty if the entity is within
     * a standalone account.
     */
    Optional<Long> billingFamilyId();

    /**
     * The OID of the region containing the entity.
     * @return The OID of the region containing the entity.
     */
    long regionOid();

    /**
     * The OID of the availability zone containing the entity or {@link Optional#empty()}, if the entity
     * is not deployed within an AZ.
     * @return The OID of the availability zone containing the entity or {@link Optional#empty()},
     * if the entity is not deployed within an AZ.
     */
    @Nonnull
    Optional<Long> availabilityZoneOid();

    /**
     * The OID of the service provider containing the entity.
     * @return The OID of the service provider containing the entity.
     */
    long serviceProviderOid();

    /**
     * A map of the classification to cloud tier demand.
     * @return A map of the classification to cloud tier demand.
     */
    @Nonnull
    Map<DemandClassification, Set<DemandTimeSeries>> classifiedCloudTierDemand();

    /**
     * The allocated cloud tier of this entity.
     * @return The allocated cloud tier of this entity.
     */
    @Nonnull
    Optional<DemandTimeSeries> allocatedCloudTierDemand();

    /**
     * Whether the entity is currently considered terminated.
     * @return True, if the entity is currently considered terminated.
     */
    @Default
    default boolean isTerminated() {
        return false;
    }

    /**
     * Whether the entity is currently considered suspended.
     * @return True, if the entity is currently considered suspended.
     */
    @Default
    default boolean isSuspended() {
        return false;
    }

    /**
     * Checks whether this entity aggregate contains any associated cloud tier demand.
     * @return True, if {@link #classifiedCloudTierDemand()} is not empty.
     */
    @Derived
    default boolean hasClassifiedDemand() {
        return classifiedCloudTierDemand().size() > 0;
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
     * A builder class for {@link ClassifiedEntityDemandAggregate} instances.
     */
    class Builder extends ImmutableClassifiedEntityDemandAggregate.Builder {}

    /**
     * Represents multiple {@link TimeInterval} instances in which the same {@link CloudTierDemand}
     * was recorded.
     */
    @Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
    @Immutable
    interface DemandTimeSeries {

        /**
         * The cloud tier demand.
         * @return The cloud tier demand.
         */
        @Nonnull
        CloudTierDemand cloudTierDemand();

        /**
         * A time series of all {@link TimeInterval} instances in which the {@link #cloudTierDemand()}
         * was recorded.
         * @return A time series of all the demand intervals.
         */
        @Auxiliary
        @Nonnull
        TimeSeries<TimeInterval> demandIntervals();

        /**
         * The total aggregate duration of this {@link DemandTimeSeries}.
         * @return The total aggregate duration of this {@link DemandTimeSeries}.
         */
        @Auxiliary
        @Derived
        @Nonnull
        default Duration duration() {
            return demandIntervals().stream()
                    .map(TimeInterval::duration)
                    .reduce(Duration.ZERO, Duration::plus);
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
         * A builder class for {@link DemandTimeSeries} instances.
         */
        class Builder extends ImmutableDemandTimeSeries.Builder {}
    }
}
