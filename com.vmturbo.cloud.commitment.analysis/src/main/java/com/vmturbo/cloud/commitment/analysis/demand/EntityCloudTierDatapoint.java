package com.vmturbo.cloud.commitment.analysis.demand;

import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * A datapoint representing the allocation of an entity on a cloud tier for a single point in time.
 * @param <CLOUD_TIER_DEMAND_TYPE> The cloud tier demand type. For a compute tier, this may represent
 *                                the tier OID, OS, and tenancy. For a DB server tier, this may represent
 *                                something like the tier OID, DB engine, DB version, etc.
 */
public interface EntityCloudTierDatapoint<CLOUD_TIER_DEMAND_TYPE> {

    /**
     * The entity OID.
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
     * is not deployed to an AZ.
     *
     * @return The OID of the availability zone containgin the entity or {@link Optional#empty()},
     * if the entity is not deployed to an AZ.
     */
    @Nonnull
    Optional<Long> availabilityZoneOid();

    /**
     * The OID of the service provider containing the entity.
     * @return The OID of the service provider containing the entity.
     */
    long serviceProviderOid();

    /**
     * The cloud tier demand, representing the attributes of workload demand for a particular type of
     * cloud tier demand. For example, for compute tier demand, the attributes will be the cloud tier
     * OID, the os type, and the tenancy.
     * @return The cloud tier demand of the entity.
     */
    @Nonnull
    CLOUD_TIER_DEMAND_TYPE cloudTierDemand();
}
