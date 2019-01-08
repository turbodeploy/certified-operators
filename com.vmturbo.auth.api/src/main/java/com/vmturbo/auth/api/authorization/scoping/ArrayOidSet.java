package com.vmturbo.auth.api.authorization.scoping;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.PrimitiveIterator;
import java.util.PrimitiveIterator.OfLong;
import java.util.Set;
import java.util.stream.LongStream;

/**
 * An {@link OidFilter} that is backed by a simple long array. This array is sorted for search
 * efficiency.
 *
 * The array shouldn't contain any negative value, and the entries should be unique.
 */
public class ArrayOidSet implements OidSet {

    private long[] oids;

    /**
     * NOTE: this does NOT copy the array into the object. The Oid Set will wrap the passed-in array.
     * If you want an ArrayOidSet containing a copy of the array, you will need to pass the copy in
     * as the input param.
     *
     * @param sourceOids
     */
    public ArrayOidSet(long[] sourceOids) {
        oids = sourceOids;
        // sort the oids
        Arrays.sort(oids);
        // TODO -- if we want more robustness, we can check for duplicate values or negative values
        // in the array and either clean the data or throw an error.
    }

    /**
     * Constructor that converts from a Collection to our primitive array type.
     *
     * @param sourceOids
     */
    public ArrayOidSet(Collection<Long> sourceOids) {
        oids = new long[sourceOids.size()];
        int index = 0;
        for (Long oid : sourceOids) {
            oids[index++] = oid;
        }
        Arrays.sort(oids);
    }

    @Override
    public boolean containsAll() {
        return false;
    }

    @Override
    public boolean contains(final long oid) {
        // use binary search on the array.
        return (Arrays.binarySearch(oids, oid) >= 0);
    }

    @Override
    public boolean contains(final Collection<Long> oids) {
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

    @Override
    public OidSet filter(final long[] inputOids) {
        // this may be a little wasteful, but we are going to do this in two steps:
        //
        // 1) create an array with the same length as the input, but populated with only the oids
        // matching the filter criteria. we do this because we don't know how many oids should be in
        // the final result.
        //
        // 2) we will copy the matching oids into a new array of the correct length, then return this
        // to the caller.
        //
        // can optimize this later if the temporary memory wasted on the extra structure is a problem.
        if (inputOids == null) {
            return EMPTY_OID_SET;
        }

        long[] temp = new long[inputOids.length];
        int numMatches = 0;
        for (int x = 0; x < inputOids.length; x++) {
            if (contains(inputOids[x])) {
                // passes the filter -- add it to the matched list.
                temp[numMatches++] = inputOids[x];
            }
        }
        // copy the matched set to the final output and return
        return new ArrayOidSet(Arrays.copyOf(temp, numMatches));
    }

    @Override
    public OidSet filter(final OidSet inputSet) {
        // we're going to do a copy / truncate as we did in the long[] version of filter(). Can
        // optimize later if needed.
        long[] temp = new long[inputSet.size()];
        int numMatches = 0;
        OfLong iterator = inputSet.iterator();
        while (iterator.hasNext()) {
            long nextOid = iterator.nextLong();
            if (contains(nextOid)) {
                temp[numMatches++] = nextOid;
            }
        }
        // copy the matched set to the final output and return
        return new ArrayOidSet(Arrays.copyOf(temp, numMatches));
    }

    @Override
    public Set<Long> filter(final Set<Long> inputOids) {
        Set<Long> retVal = new HashSet<>();
        for (Long oid : inputOids) {
            if (contains(oid)) {
                retVal.add(oid);
            }
        }
        return retVal;
    }


    @Override
    public PrimitiveIterator.OfLong iterator() {
        return LongStream.of(oids).iterator();
    }

    @Override
    public int size() {
        return oids.length;
    }

    @Override
    public OidSet union(final OidSet other) {
        // creating the union of the two sets is not going to be super-efficient.
        // we can either create a new worst-case-sized array with the merge results, and
        // then copy to a correctly-sized output array, or we run two passes, the first to count
        // the number of unique entries (so we can correctly size the output array) and the second
        // to copy the results into the output structure.
        //
        // The first approach is simpler to write, so going with that approach for now. We can
        // optimize later, if necessary.
        if (other == null) {
            return EMPTY_OID_SET;
        }

        int tempSize = size() + other.size();
        long[] temp = Arrays.copyOf(oids, tempSize);
        // concat the oids from the other set into the temp array
        PrimitiveIterator.OfLong iterator = other.iterator();
        for (int x = size(); x < tempSize; x++) {
            temp[x] = iterator.nextLong();
        }
        // sort the concatenated arrays.
        Arrays.sort(temp);
        // remove duplicates
        long lastOid = -1; // negative oids are invalid
        int currentIndex = 0; // where to place the next unique value
        for (int x = 0; x < tempSize; x++) {
            if (temp[x] != lastOid) {
                // shift this unique oid into the next available slot
                temp[currentIndex++] = temp[x];
                lastOid = temp[x];
            }
        }

        // copy the unique sub-array to a fresh new structure.
        long[] output = Arrays.copyOf(temp, currentIndex);
        return new ArrayOidSet(output);
    }
}
