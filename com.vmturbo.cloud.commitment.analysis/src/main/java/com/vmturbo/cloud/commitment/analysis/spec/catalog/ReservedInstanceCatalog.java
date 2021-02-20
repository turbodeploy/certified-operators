package com.vmturbo.cloud.commitment.analysis.spec.catalog;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;

/**
 * A {@link CloudCommitmentCatalog} for resolving available RI specs.
 */
public interface ReservedInstanceCatalog extends CloudCommitmentCatalog<ReservedInstanceSpec> {

    /**
     * A factory interface for constructing {@link ReservedInstanceCatalog} instances.
     */
    interface ReservedInstanceCatalogFactory extends CloudCommitmentCatalog.CloudCommitmentCatalogFactory<ReservedInstanceSpec> {

        /**
         * {@inheritDoc}.
         */
        @Override
        ReservedInstanceCatalog createAccountRestrictedCatalog(@Nonnull Collection<Long> accountOids);
    }
}
