package com.vmturbo.components.common.identity;

import java.util.Collection;
import java.util.PrimitiveIterator;
import java.util.PrimitiveIterator.OfLong;
import java.util.function.LongConsumer;

import org.apache.commons.lang.NotImplementedException;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * An OidSet that is backed by a {@link Roaring64NavigableMap}
 */
public class RoaringBitmapOidSet implements OidSet {
    private final Roaring64NavigableMap roaringBitmap;

    public RoaringBitmapOidSet(long[] oids) {
        roaringBitmap = Roaring64NavigableMap.bitmapOf(oids != null ? oids : new long[0]);
    }

    public RoaringBitmapOidSet(Collection<Long> oids) {
        roaringBitmap = new Roaring64NavigableMap();
        oids.forEach(roaringBitmap::addLong);
    }
    /**
     * Create an instance based on a pre-created {@link Roaring64NavigableMap}.
     * @param source
     */
    RoaringBitmapOidSet(Roaring64NavigableMap source) {
        roaringBitmap = source;
    }

    @Override
    public OfLong iterator() {
        return new RoaringBitmapOidSetIterator(roaringBitmap);
    }

    @Override
    public int size() {
        // this may get truncated to an int, but our OidSet structure doesn't support sets greater
        // than int capacity anyways.
        return (int) roaringBitmap.getLongCardinality();
    }

    @Override
    public int hashCode() {
        return defaultHashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return defaultEquals(obj);
    }



    /**
     * {@inheritDoc}
     *
     * @param other
     * @return
     */
    @Override
    public OidSet union(final OidSet other) {
        if (other == null) {
            return EMPTY_OID_SET;
        }
        // create a roaring bitmap containing the union results
        Roaring64NavigableMap resultMap = new Roaring64NavigableMap();
        other.iterator().forEachRemaining((LongConsumer) resultMap::addLong);
        // perform the union using the "or" method on the roaring bitmap. this modifies the bitmap
        // being operated on.
        resultMap.or(roaringBitmap);

        // return the union structure
        RoaringBitmapOidSet result = new RoaringBitmapOidSet(resultMap);
        return result;
    }

    @Override
    public boolean containsAll() {
        return false;
    }

    @Override
    public boolean contains(final long oid) {
        return roaringBitmap.contains(oid);
    }

    @Override
    public OidSet filter(final long[] inputOids) {
        if (inputOids == null || inputOids.length == 0) {
            return EMPTY_OID_SET;
        }

        Roaring64NavigableMap resultSet = new Roaring64NavigableMap();
        for (int x = 0; x < inputOids.length ; x++) {
            if (roaringBitmap.contains(inputOids[x])) {
                resultSet.add(inputOids[x]);
            }
        }

        return new RoaringBitmapOidSet(resultSet);
    }

    @Override
    public OidSet filter(final OidSet inputSet) {
        if (inputSet == null) {
            return EMPTY_OID_SET;
        }

        Roaring64NavigableMap resultSet = new Roaring64NavigableMap();
        OfLong iterator = inputSet.iterator();
        while (iterator.hasNext()) {
            long nextOid = iterator.nextLong();
            if (contains(nextOid)) {
                resultSet.add(nextOid);
            }
        }

        return new RoaringBitmapOidSet(resultSet);
    }

    /**
     * This class provides a primitive iterator wrapper around the roaring bitmap's own iterator.
     *
     */
    private class RoaringBitmapOidSetIterator implements PrimitiveIterator.OfLong {
        final LongIterator rbIterator;

        public RoaringBitmapOidSetIterator(Roaring64NavigableMap r) {
            rbIterator = r.getLongIterator();
        }

        @Override
        public boolean hasNext() {
            return rbIterator.hasNext();
        }

        @Override
        public long nextLong() {
            return rbIterator.next();
        }

        @Override
        public void remove() {
            // not implemented
            throw new NotImplementedException();
        }
    }
}
