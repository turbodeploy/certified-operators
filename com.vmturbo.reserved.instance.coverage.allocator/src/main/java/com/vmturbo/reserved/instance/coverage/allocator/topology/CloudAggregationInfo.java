package com.vmturbo.reserved.instance.coverage.allocator.topology;

import java.util.OptionalLong;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A data class containing aggregation info for cloud entities. Aggregation info includes the relationship
 * of an entity to cloud infrastructure and aggregating entities (e.g. account).
 */
@HiddenImmutableImplementation
@Immutable
public interface CloudAggregationInfo {

    /**
     * The billing family ID.
     * @return The billing family ID. May be empty, if the corresponding entity is not related to
     * a billing family.
     */
    @Nonnull
    OptionalLong billingFamilyId();

    /**
     * The account OID.
     * @return The account OID.
     */
    long accountOid();

    /**
     * The region OID.
     * @return The region OID.
     */
    long regionOid();

    /**
     * The zone OID.
     * @return The zone OID. May be empty if the entity is not located within an availability zone.
     */
    @Nonnull
    OptionalLong zoneOid();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for creating {@link CloudAggregationInfo} instances.
     */
    class Builder extends ImmutableCloudAggregationInfo.Builder {}
}
