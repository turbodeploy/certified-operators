package com.vmturbo.matrix.component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * The {@link ServiceEntityOIDMapper} implements SE to OID bidirectional mapper.
 */
public class ServiceEntityOIDMapper {
    /**
     * The SE represented by UUID to OID map.
     */
    @VisibleForTesting final Map<String, Long> seToOid_;

    /**
     * The OID to SE represented by UUID map.
     */
    @VisibleForTesting final Map<Long, String> oidToSe_;

    /**
     * The lock.
     */
    private final transient ReentrantReadWriteLock lock_;

    /**
     * Constructs tbe mapper.
     */
    ServiceEntityOIDMapper() {
        seToOid_ = new HashMap<>();
        oidToSe_ = new HashMap<>();
        lock_ = new ReentrantReadWriteLock(true);
    }

    /**
     * Creates a copy.
     *
     * @return The deep copy of the mapper.
     */
    public @Nonnull ServiceEntityOIDMapper copy() {
        ServiceEntityOIDMapper result = new ServiceEntityOIDMapper();
        lock_.readLock().lock();
        try {
            result.seToOid_.putAll(new HashMap<>(seToOid_));
            result.oidToSe_.putAll(new HashMap<>(oidToSe_));
        } finally {
            lock_.readLock().unlock();
        }
        return result;
    }

    /**
     * Clears all content.
     */
    public void clear() {
        lock_.writeLock().lock();
        try {
            seToOid_.clear();
            oidToSe_.clear();
        } finally {
            lock_.writeLock().unlock();
        }
    }

    /**
     * Maps SE represented by UUID to a newly generated OID.
     * This is a NO-OP if the mapping for the UUID already exists.
     *
     * @param uuid The SE uuid.
     * @return The newly generated OID or an existing one if mapping for UUID exists.
     */
    public @Nonnull Long map(final @Nonnull String uuid) {
        Long oid;
        lock_.writeLock().lock();
        try {
            oid = seToOid_.get(uuid);
            if (oid == null) {
                oid = IdentityGenerator.next();
                seToOid_.put(uuid, oid);
                oidToSe_.put(oid, uuid);
            }
        } finally {
            lock_.writeLock().unlock();
        }
        return oid;
    }

    /**
     * Remaps SE represented by UUID to OID.
     *
     * @param uuid The SE uuid.
     * @param oid  The OID.
     */
    public void remap(final @Nonnull String uuid, final @Nonnull Long oid) {
        lock_.writeLock().lock();
        try {
            final Long oidOld = seToOid_.get(uuid);
            if (oidOld != null) {
                seToOid_.put(uuid, oid);
                oidToSe_.remove(oidOld);
                oidToSe_.put(oid, uuid);
            }
        } finally {
            lock_.writeLock().unlock();
        }
    }

    /**
     * Retrieves the OID for a given uuid.
     *
     * @param uuid The UUID.
     * @return The OID or {@link Optional#empty()} if not found.
     */
    public @Nonnull Optional<Long> uuidToOid(final @Nonnull String uuid) {
        Optional<Long> result;
        lock_.readLock().lock();
        try {
            result = Optional.ofNullable(seToOid_.get(uuid));
        } finally {
            lock_.readLock().unlock();
        }
        return result;
    }

    /**
     * Retrieves the UUID for a given OID.
     *
     * @param oid The oid.
     * @return The UUID or {@link Optional#empty()} if not found.
     */
    public @Nonnull Optional<String> oidToUuid(final @Nonnull Long oid) {
        Optional<String> result;
        lock_.readLock().lock();
        try {
            result = Optional.ofNullable(oidToSe_.get(oid));
        } finally {
            lock_.readLock().unlock();
        }
        return result;
    }
}
