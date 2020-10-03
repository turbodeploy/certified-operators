package com.vmturbo.reserved.instance.coverage.allocator.matcher;

import java.util.OptionalLong;

import javax.annotation.Nonnull;

/**
 * A base coverage key, representing the criteria for matching a cloud commitment to an entity.
 */
public interface CoverageKey {

    /**
     * The billing family ID.
     * @return The billing family ID.
     */
    @Nonnull
    OptionalLong billingFamilyId();

    /**
     * The account OID.
     * @return The account OID.
     */
    @Nonnull
    OptionalLong accountOid();

    /**
     * The region OID.
     * @return The region OID.
     */
    @Nonnull
    OptionalLong regionOid();

    /**
     * The zone OID.
     * @return The zone OID.
     */
    @Nonnull
    OptionalLong zoneOid();

}
