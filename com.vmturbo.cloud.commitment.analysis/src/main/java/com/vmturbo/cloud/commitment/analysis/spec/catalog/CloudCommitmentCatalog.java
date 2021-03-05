package com.vmturbo.cloud.commitment.analysis.spec.catalog;

import java.util.Collection;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;

/**
 * Represents all available cloud commitment specs available within the catalog for a specific scope. In general,
 * this will be all commitment specs across an entire billing family. The catalog is responsible for verifying
 * pricing availability for each commitment specification, in order to filter those specs that have either been
 * retired or are defined for the commitment inventory.
 * @param <SPEC_TYPE> The cloud commitment spec type.
 */
public interface CloudCommitmentCatalog<SPEC_TYPE> {

    /**
     * Resolves the set of {@link SpecAccountGrouping} within the catalog for the regional and organizational
     * constraints provided in the {@link SpecCatalogKey} and {@code regionOid}.
     * @param catalogKey The catalog key, identifying the organizational constraints used
     *                   to filter the catalog.
     * @param regionOid The target region OID.
     * @return The set {@link SpecAccountGrouping} instances, each of which defines a set of specs and
     * a group of accounts those specs are available in.
     */
    @Nonnull
    Set<SpecAccountGrouping<SPEC_TYPE>> getRegionalSpecs(@Nonnull SpecCatalogKey catalogKey,
                                                         long regionOid);

    /**
     * A factory class for producing {@link CloudCommitmentCatalog} instances.
     * @param <SPEC_TYPE> The commitment specification type.
     */
    interface CloudCommitmentCatalogFactory<SPEC_TYPE> {

        /**
         * Creates a {@link CloudCommitmentCatalog}, in which the catalog of available commitment specs
         * are constrained to only those available in {@code accountOids}.
         * @param accountOids The purchasing account OIDs. If this collection is empty, all accounts
         *                    will be considered.
         * @return The new {@link CloudCommitmentCatalog} instance.
         */
        CloudCommitmentCatalog<SPEC_TYPE> createAccountRestrictedCatalog(@Nonnull Collection<Long> accountOids);
    }

    /**
     * Represents a set of specifications available within the same set of accounts. The set of accounts
     * are those with the same catalog (price table).
     * @param <SPEC_TYPE> The commitment spec type.
     */
    @HiddenImmutableTupleImplementation
    @Immutable(builder = false)
    interface SpecAccountGrouping<SPEC_TYPE> {

        /**
         * The set of cloud commitment specs.
         * @return An immutable set of cloud commitment specs.
         */
        @Nonnull
        Set<SPEC_TYPE> commitmentSpecs();

        /**
         * The set of account OIDs in which the {@link #commitmentSpecs()} are available.
         * @return An immutable set of account OIDs.
         */
        @Nonnull
        Set<Long> availableAccountOids();

        /**
         * Constructs a new {@link SpecAccountGrouping} instance.
         * @param commitmentSpecs The set of commitment specs.
         * @param availableAccountOids The set of accounts the specs are available in.
         * @param <T> The type of the commitment specs.
         * @return The newly constructed {@link SpecAccountGrouping} instance.
         */
        @Nonnull
        static <T> SpecAccountGrouping<T> of(@Nonnull Set<T> commitmentSpecs,
                                             @Nonnull Set<Long> availableAccountOids) {
            return SpecAccountGroupingTuple.of(commitmentSpecs, availableAccountOids);
        }
    }


}
