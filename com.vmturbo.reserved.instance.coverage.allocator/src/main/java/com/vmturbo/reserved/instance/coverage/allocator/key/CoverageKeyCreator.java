package com.vmturbo.reserved.instance.coverage.allocator.key;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * A creator interface for {@link CoverageKey} instances
 */
public interface CoverageKeyCreator {

    /**
     * Attempts to create a {@link CoverageKey} from a topology entity. Key creation will fail
     * if the topology is malformed (e.g. if the entity is not connected to a region).
     *
     * @param entityOid The oid of the entity
     * @return An optional {@link CoverageKey}, if one was successfully created. {@link Optional#empty()},
     * if key creation failed
     */
    Optional<CoverageKey> createKeyForEntity(long entityOid);

    /**
     * Attempts to create a set {@link CoverageKey} from an RI. Key creation will fail
     * if the topology is malformed (e.g. if topology does not contain the RI's region).
     *
     * @param riOid The oid of the RI
     * @return The set of keys that may be covered by the RI. Multiple keys will be returned if the
     * RI is scoped to multiple accounts.
     */
    Set<CoverageKey> createKeysForReservedInstance(long riOid);

    /**
     * A factory for creating an instance of {@link CoverageKeyCreator}
     */
    @FunctionalInterface
    interface CoverageKeyCreatorFactory {

        /**
         * Creates a new instance of {@link CoverageKeyCreator}
         *
         * @param coverageTopology The {@link CoverageTopology} used to resolve oid references for both
         *                         entities and RIs during {@link CoverageKey} creation
         * @param keyCreationConfig An instance of {@link CoverageKeyCreationConfig}, defining how key
         *                          material for {@link CoverageKey} should be extracted from an
         *                          entity or RI
         * @return A new instance of {@link CoverageKeyCreator}
         */
        @Nonnull
        CoverageKeyCreator newCreator(@Nonnull CoverageTopology coverageTopology,
                                      @Nonnull CoverageKeyCreationConfig keyCreationConfig);
    }
}
