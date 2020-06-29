package com.vmturbo.cloud.commitment.analysis.demand;

import java.time.Instant;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;

/**
 * A mapping of an entity to cloud tier over a period of time, as represented by the start and end
 * times of the mapping.
 *
 * @param <CLOUD_TIER_DEMAND_TYPE> The type of cloud tier demand stored within the mapping.
 */
@Immutable
public interface EntityCloudTierMapping<CLOUD_TIER_DEMAND_TYPE> {

    /**
     * The start time of the mapping between the entity and cloud tier.
     * @return The start time in UTC.
     */
    @Nonnull
    Instant startTime();

    /**
     * The end time of the mapping between the entity and cloud tier. The end time may represent the
     * latest recorded cloud tier mapping for the assocaited entity, that the entity was deallocated,
     * or that the entity was moved to another cloud tier.
     *
     * @return The ent time of the mapping in UTC.
     */
    @Nonnull
    Instant endTime();

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
     * The cloud tier demand of the mapping. The demand type will be specific to the type of the cloud
     * tier.
     *
     * @return The cloud tier demand of the mapping.
     */
    @Nonnull
    CLOUD_TIER_DEMAND_TYPE cloudTierDemand();

    @Nonnull
    CloudTierType cloudTierType();

}
