package com.vmturbo.extractor.models;

import net.jpountz.xxhash.XXHashFactory;

/**
 * Class that contains a few items needed for hashing.
 *
 * <h3>What we use hashing for</h3>
 *
 * <p>Our primary data tables are `entity` and `metric`. The latter contain time series data for
 * various metrics relating to entities found in topologies. The `entity` table is not configured
 * with time series data, but in reality, even entity data does change, albeit rather slowly in
 * most circumstances.</p>
 *
 * <p>Every metric record has a specific timestamp corresponding to the timestamp of the topology
 * from which it was obtained. Entity records apply to multiple timestamps, based on their
 * changing values. To allow joining metric records with the correct entity records, we compute
 * a hash of potentially varying entity columns, and include the hash in each entity record.
 * The same hash values are stored in the metric table. Thus at any given time, there may be
 * entity OID values that appear in multiple entity records, but all with different hash values.</p>
 */
public class HashUtil {

    /**
     * Factory for XXHash value generation.
     *
     * <p>The "unsafe" instance factory makes access a pure java implementation that makes use of
     * the {@link sun.misc.Unsafe} class to speed calculations.</p>
     */
    public static final XXHashFactory XXHASH_FACTORY = XXHashFactory.unsafeInstance();

    /** Seed value for use with this factory (must always use same value!). */
    public static final long XXHASH_SEED = 0x4e723825ad398f89L;

    private HashUtil() {
    }
}
