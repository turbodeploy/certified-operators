package com.vmturbo.auth.api.authorization.scoping;

import java.util.Collection;
import java.util.Set;

/**
 * The OidFilter provides a filter on oid's.
 *
 * The oids are represnted as primitive longs for efficiency reasons.
 *
 *
 */
public interface OidFilter {
    /**
     * Attribute indicating whether the filter includes all oids or not. If
     * containsAll is true, then the filter is effectively a no-op and doesn't need to be called.
     *
     * @return
     */
    boolean containsAll();

    /**
     * Attribute indicating whether the filter actually filters anything out or not. It's the
     * opposite answer from "containsAll". If "hasRestrictions" is true then the filter actually
     * does something.
     *
     * @return true, if the filter may filter results out.
     */
    default boolean hasRestrictions() {
        return ! containsAll();
    }

    /**
     * Given an oid, return true or false if the oid passes the filter.
     *
     * @param oid
     * @return
     */
    boolean contains(long oid);

    /**
     * Given a collection of oids, return true if all oids in the collection pass the filter.
     *
     * @param oids
     * @return true, if all oids in the collection pass the filter. false, if any do not.
     */
    boolean contains(Collection<Long> oids);

    /**
     * Given an array of oids, return an array of oids that pass the filter.
     *
     * @param inputOids
     * @return
     */
    OidSet filter(long[] inputOids);

    /**
     * Given an {@link OidSet}, return a new OidSet containing the values that pass this filter.
     * This is effectively an intersection operation.
     *
     * @param inputSet
     * @return
     */
    OidSet filter(OidSet inputSet);

    /**
     * Convenience method for filtering a {@link Set} of {@link Long}. You should try to use the
     * primitive filter(long[]) method or filter(OidSet) methods when possible, since they will use
     * less memory overhead and potentially better performance. But if you want input and output in
     * {@Set} objects then you might as well use this version to keep things simple.
     *
     * @param inputOids
     * @return
     */
    Set<Long> filter(Set<Long> inputOids);

    /**
     * The AllOidsFilter doesn't filter any oids out. It's basically a no-op filter.
     */
    public class AllOidsFilter implements OidFilter {
        public static final AllOidsFilter ALL_OIDS_FILTER = new AllOidsFilter();

        @Override
        public boolean containsAll() {
            return true;
        }

        @Override
        public boolean contains(final long oid) {
            return true;
        }

        @Override
        public boolean contains(final Collection<Long> oids) {
            return true;
        }

        @Override
        public OidSet filter(final long[] inputOids) {
            return new ArrayOidSet(inputOids);
        }

        /**
         * {@inheritDoc}
         * <br>
         * NOTE: This method will return a reference to the original input structure. Since there is no
         * filtering being applied, then the input and output are the same. We could also copy the values
         * into a new output structure, which would make for a cleaner contract, but I don't know if it's
         * worth the extra overhead of creating the copy. For now, going with the simpler / more
         * efficient approach.
         */
        @Override
        public OidSet filter(final OidSet inputSet) {
            return inputSet;
        }

        /**
         * {@inheritDoc}
         * <br>
         * NOTE: This method will return a reference to the original input structure. Since there is no
         * filtering being applied, then the input and output are the same. We could also copy the values
         * into a new output structure, which would make for a cleaner contract, but I don't know if it's
         * worth the extra overhead of creating the copy. For now, going with the simpler / more
         * efficient approach.
         */
        @Override
        public Set<Long> filter(final Set<Long> inputOids) {
            return inputOids;
        }
    }
}

