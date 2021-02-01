package com.vmturbo.cost.component.entity.scope;

import java.time.Instant;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Entity cloud scope is the immutable attributes of a cloud entity that may be used in a request query
 * to aggregate entity statistics.
 */
@HiddenImmutableImplementation
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

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link EntityCloudScope} instances.
     */
    class Builder extends ImmutableEntityCloudScope.Builder {}
}
