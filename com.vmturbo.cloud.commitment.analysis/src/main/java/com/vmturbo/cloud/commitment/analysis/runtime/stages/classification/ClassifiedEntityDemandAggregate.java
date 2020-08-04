package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.CloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;

/**
 * Represents all {@link CloudTierDemand} associated with a single entity, aggregated and classified.
 * @param <CLASSIFICATION_TYPE> The classification of this aggregate (either allocated or projected).
 */
@Immutable
public interface ClassifiedEntityDemandAggregate<CLASSIFICATION_TYPE> {

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
    Map<CLASSIFICATION_TYPE, Set<DemandTimeSeries>> classifiedCloudTierDemand();

    /**
     * Represents multiple {@link TimeInterval} instances in which the same {@link CloudTierDemand}
     * was recorded.
     */
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
        @Nonnull
        TimeSeries<TimeInterval> demandIntervals();

        /**
         * The total aggregate duration of this {@link DemandTimeSeries}.
         * @return The total aggregate duration of this {@link DemandTimeSeries}.
         */
        @Nonnull
        @Default
        default Duration duration() {
            return demandIntervals().stream()
                    .map(TimeInterval::duration)
                    .reduce(Duration.ZERO, Duration::plus);
        }
    }
}
