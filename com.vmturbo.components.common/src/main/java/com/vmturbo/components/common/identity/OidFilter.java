package com.vmturbo.components.common.identity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.PrimitiveIterator;
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
     * Given a string oid, check if it passes the filter.
     *
     * @param stringOid
     * @return
     */
    default boolean contains(String stringOid) {
        // non-numeric strings will trigger NumberFormatExceptions -- not attempting to handle these
        // here.
        return contains(Long.valueOf(stringOid));
    }

    /**
     * Given a collection of oids, return true if all oids in the collection pass the filter.
     *
     * @param oids
     * @return true, if all oids in the collection pass the filter. false, if any do not.
     */
    default boolean contains(Collection<Long> oids) {
        if (oids == null) {
            return true;
        }
        // return false on the first oid that doesn't match the filter.
        for (Long oid: oids) {
            if (! contains(oid)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Given an {@link OidSet} of oids, return true if all of the oids in the set pass the filter.
     *
     * @param oids the OidSet to compare
     * @return true, if all oids in the collection pass the filter. false, if any do not.
     */
    default boolean contains(OidSet oids) {
        if (oids == null) {
            return true;
        }
        // return false on the first oid that doesn't match the filter.
        PrimitiveIterator.OfLong iterator = oids.iterator();
        while (iterator.hasNext()) {
            if (!this.contains(iterator.nextLong())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Given a collection of oids, return true if any oid in the collection pass the filter.
     *
     * @param oids the collection of oids to check.
     * @return true, if any oid in the collection passes the filter. false, if none pass.
     */
    default boolean containsAny(Collection<Long> oids) {
        if (oids == null) {
            return false;
        }

        // return true if any are contained
        for (Long oid: oids) {
            if (contains(oid)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Given an {@link OidSet} of oids, return true if any oid in the OidSet passes the filter.
     *
     * @param oids the set of oids to check.
     * @return true, if any oid in the collection passes the filter. false, if none pass.
     */
    default boolean containsAny(OidSet oids) {
        if (oids == null) {
            return false;
        }
        // return true if any are contained
        PrimitiveIterator.OfLong iterator = oids.iterator();
        while (iterator.hasNext()) {
            if (this.contains(iterator.nextLong())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Utility function for checking "contains" on a collection of oids represented as strings. If
     * any entry cannot be converted, the result will be "false". Otherwise, the result will be
     * as described in the contains(Collection<Long>) method.
     *
     * @param stringOids
     * @return
     */
    default boolean containsStringOids(Collection<String> stringOids) {
        // convert each string to a long and check contains(). If there are any parsing/conversion
        // errors, return false.
        try {
            for (String stringOid : stringOids) {
                if (!contains(Long.valueOf(stringOid))) {
                    return false;
                }
            }

        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

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
    default Set<Long> filter(Set<Long> inputOids) {
        Set<Long> retVal = new HashSet<>();
        for (Long oid : inputOids) {
            if (contains(oid)) {
                retVal.add(oid);
            }
        }
        return retVal;
    }

    default List<Long> filter(List<Long> inputOids) {
        List<Long> retVal = new ArrayList<>();
        for (Long oid : inputOids) {
            if (contains(oid)) {
                retVal.add(oid);
            }
        }
        return retVal;
    }

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
            return new RoaringBitmapOidSet(inputOids);
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

