package com.vmturbo.components.common.identity;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.PrimitiveIterator;
import java.util.PrimitiveIterator.OfLong;
import java.util.Set;
import java.util.stream.LongStream;

import org.apache.commons.lang.NotImplementedException;

/**
 * OidSet extends OidFilter by adding the ability to iterate over the members, perform union
 * operations, as well as get a count of the number of members.
 */
public interface OidSet extends OidFilter, Iterable<Long> {
    public static final OidSet EMPTY_OID_SET = new EmptyOidSet();

    /**
     * Give access to a java iterator.
     *
     * @return
     */
    PrimitiveIterator.OfLong iterator();

    int size();

    /**
     * A default implementation of hashcode() for an OidSet. This is designed to be order-independent.
     *
     * @return hashcode
     */
    default int defaultHashCode() {
        // the roaring bitmap hash code is not stable. We'll provide one here that will work
        // like the standard Set hashcode does.
        int hash = 0;
        PrimitiveIterator.OfLong iterator = iterator();
        while (iterator.hasNext()) {
            long l = iterator.next();
            hash += (int)l ^ (l >>> 32);
        }
        return hash;
    }



    /**
     * A default implementation of equals() for comparing two OidSets. Two OidSets will be considered
     * equals if the members of the set are the same.
     *
     * @param obj the other object to compare against.
     * @return true, if the members of the oidsets are the same. False, if obj is null, is not an
     * OidSet, or contains a different set of members.
     */
    default boolean defaultEquals(final Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (!(obj instanceof OidSet)) {
            return false;
        }

        // we're going to have a tolerant equals -- as long as all the members are the same, we'll
        // call them equal
        OidSet otherSet = (OidSet)obj;
        if (this.contains(otherSet) && (this.size() == otherSet.size())) {
            return true;
        }
        return false;
    }



    /**
     * Merge this OidSet with another one, returning the result.
     *
     * There is no need for an intersection operation, because the filter() method is effectively an
     * intersection.
     *
     * @param other
     * @return
     */
    OidSet union(OidSet other);

    /**
     * Converts an OidSet to a {@link Set<Long>}.
     *
     * @return
     */
    default Set<Long> toSet() {
        // create a simple HashSet<Long> and populate it via the iterator.
        Set<Long> retVal = new HashSet<>(size());
        OfLong iterator = iterator();
        while (iterator.hasNext()) {
            retVal.add(iterator.nextLong());
        }
        return retVal;
    }

    /**
     * The special "Empty" OidSet doesn't contain any entries. Any intersections on it will produce
     * another empty set.
     *
     */
    public static class EmptyOidSet implements OidSet {
        @Override
        public OfLong iterator() {
            return LongStream.empty().iterator();
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public OidSet union(final OidSet other) {
            if (other == null) return this;
            return other;
        }

        @Override
        public boolean containsAll() {
            return false;
        }

        @Override
        public boolean contains(final long oid) {
            return false;
        }

        @Override
        public boolean contains(final Collection<Long> oids) {
            return false;
        }

        @Override
        public OidSet filter(final long[] inputOids) {
            return OidSet.EMPTY_OID_SET;
        }

        @Override
        public OidSet filter(final OidSet inputSet) {
            return OidSet.EMPTY_OID_SET;
        }

        @Override
        public Set<Long> filter(final Set<Long> inputOids) {
            return Collections.EMPTY_SET;
        }
    }

    /**
     * I hope we don't end up keeping this class. The AllOidsFilter should be all the functionality
     * we need. But I'm adding this this for convenience, to provide an object that can support the
     * OidSet interface while also representing an "all oids" case.
     */
    public static class AllOidsSet extends OidFilter.AllOidsFilter implements OidSet {
        public static final AllOidsSet ALL_OIDS_SET = new AllOidsSet();

        /**
         * We could return an iterator that just increments through the universe of long values, but
         * this method doesn't make sense to call on the "all oids" scenario. I think an error will
         * be more useful.
         *
         * @return
         */
        @Override
        public OfLong iterator() {
            throw new NotImplementedException();
        }

        /**
         * Technically we can probably return MaxLong here, but going to throw an exception instead
         * since this method really shouldn't be used on this object.
         *
         * @return
         */
        @Override
        public int size() {
            throw new NotImplementedException();
        }

        /**
         * Union with the AllOidsSet just returns the AllOidsSet.
         * @param other
         * @return
         */
        @Override
        public OidSet union(final OidSet other) {
            return ALL_OIDS_SET;
        }

        @Override
        public Set<Long> toSet() {
            throw new NotImplementedException();
        }
    }
}
