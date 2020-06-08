package com.vmturbo.cost.component.entity.scope;

import java.time.Instant;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

/**
 * Entity cloud scope is the immutable attributes of a cloud entity that may be used in a request query
 * to aggregate entity statistics.
 */
@Immutable
public interface EntityCloudScope {

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
     * is not deployed in an AZ.
     * @return The OID of the availability zone containing the entity or {@link Optional#empty()},
     * if the entity is not deployed in an AZ.
     */
    @Nonnull
    Optional<Long> availabilityZoneOid();

    /**
     * The OID of the service provider containing the entity.
     * @return The OID of the service provider containing the entity.
     */
    long serviceProviderOid();

    /**
     * The creation time of the entity cloud scope record.
     * @return The creation time of the entity cloud scope record.
     */
    @Value.Auxiliary
    @Nonnull
    Instant creationTime();
}
