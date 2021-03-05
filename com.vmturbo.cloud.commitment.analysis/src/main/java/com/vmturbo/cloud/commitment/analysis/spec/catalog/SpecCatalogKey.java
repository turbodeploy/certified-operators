package com.vmturbo.cloud.commitment.analysis.spec.catalog;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;

/**
 * A key for a commitment catalog, representing all commitment specifications available to a billing
 * family or standalone account.
 */
@HiddenImmutableTupleImplementation
@Immutable(builder = false, lazyhash = true)
public interface SpecCatalogKey {

    /**
     * The organization type (either billing family or standalone account).
     * @return The {@link OrganizationType}.
     */
    OrganizationType organizationType();

    /**
     * The organization ID. The type represented will depend on {@link #organizationType()}.
     * @return The organization ID. The type represented will depend on {@link #organizationType()}.
     */
    long organizationId();

    /**
     * Constructs a new {@link SpecCatalogKey}.
     * @param organizationType The organization type.
     * @param organizationId The organization ID.
     * @return The new {@link SpecCatalogKey} instance.
     */
    static SpecCatalogKey of(@Nonnull OrganizationType organizationType,
                             long organizationId) {
        return SpecCatalogKeyTuple.of(organizationType, organizationId);
    }

    /**
     * The organization type.
     */
    enum OrganizationType {
        BILLING_FAMILY,
        STANDALONE_ACCOUNT
    }
}
