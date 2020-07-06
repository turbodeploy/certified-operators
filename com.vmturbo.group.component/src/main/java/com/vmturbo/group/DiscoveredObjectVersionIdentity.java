package com.vmturbo.group;

import javax.annotation.Nullable;

/**
 * Identity of the discovered object, already known. This returns OID of the object already
 * existing in a store.
 */
public class DiscoveredObjectVersionIdentity {
    private final long oid;
    private final byte[] hash;

    /**
     * Constructs discovered object identity.
     *
     * @param oid OID
     * @param hash hash
     */
    public DiscoveredObjectVersionIdentity(long oid, @Nullable byte[] hash) {
        this.oid = oid;
        this.hash = hash;
    }

    /**
     * Returns Oid of the object.
     *
     * @return OID
     */
    public long getOid() {
        return oid;
    }

    /**
     * Returns object hash. The assumption is that for every single change of the object, hash
     * will have a different value. In other words: if hash in the DB is the same as hash of
     * the object received from the probe, we assume that it is the same object and it is not
     * required to update it.
     *
     * @return hash, if any
     */
    @Nullable
    public byte[] getHash() {
        return hash;
    }

    @Override
    public String toString() {
        return Long.toString(oid) + '['
                + (hash == null || hash.length == 0 ? "null" : hash[0] + "...")
                + ']';
    }
}
