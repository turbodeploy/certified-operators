package com.vmturbo.extractor.topology;

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

/**
 * This class manages a list of `long` ids (entity, group, etc.) that are relevant to a particular
 * context (like a topology), allowing the referenced entities to be identified by an `int` value,
 * permitting more compact in-memory data structures.
 *
 * <p>The integer ids ("iids") form a dense list of positive integer values, permitting a `long`
 * array to be maintained for translations back to the original `long` ids, e.g. when persisting
 * ids.</p>
 */
public class EntityIdManager {
    private final Long2IntMap oid2iid = new Long2IntOpenHashMap();
    private final LongList iid2oid = new LongArrayList();

    /**
     * Translate an oid to its iid, allocate a new iid if needed.
     *
     * @param oid oid to be translated
     * @return corresponding iid
     */
    public int toIid(long oid) {
        if (oid2iid.containsKey(oid)) {
            return oid2iid.get(oid);
        } else {
            int iid = oid2iid.size() + 1;
            oid2iid.put(oid, iid);
            iid2oid.add(oid); // will be added at position iid-1
            return iid;
        }
    }

    /**
     * Translate an iid to its oid.
     *
     * @param iid iid to be translated
     * @return corresponding oid
     * @throws IndexOutOfBoundsException if the index is not > 0 and <= oid count
     */
    public long toOid(int iid) throws IndexOutOfBoundsException {
        return iid2oid.getLong(iid - 1);
    }

    /**
     * Translate an iid to its oid.
     *
     * <p>This is like {@link #toOid(int)} but will return null instead of throwing
     * {@link IndexOutOfBoundsException} for invalid input.
     *
     * @param iid iid to be translated
     * @return corresponding oid, or null if not found
     */
    public Long toOidOrNull(int iid) {
        return iid > 0 && iid <= iid2oid.size() ? iid2oid.getLong(iid - 1) : null;
    }
}
