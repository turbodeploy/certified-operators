package com.vmturbo.topology.event.library.uptime;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.entity.scope.CloudScopeStore;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudScopeFilter;

/**
 * An in-memory implementation of {@link EntityUptimeStore}.
 */
public class InMemoryEntityUptimeStore implements EntityUptimeStore {

    private final CloudScopeStore cloudScopeStore;

    private final EntityUptime defaultUptime;

    private final ReadWriteLock uptimeDataLock = new ReentrantReadWriteLock();

    private TimeInterval uptimeWindow = TimeInterval.EPOCH;

    private Map<Long, EntityUptime> entityUptimeMap = Collections.emptyMap();

    /**
     * Constructs a new in-memory entity uptime store.
     * @param cloudScopeStore The {@link CloudScopeStore}, used to resolve entities in scope
     *                        of a query.
     * @param defaultUptime The default uptime to return, in the event the store does not have an uptime
     *                      calculation for a specific entity. This value may be null.
     */
    public InMemoryEntityUptimeStore(@Nonnull CloudScopeStore cloudScopeStore,
                                     @Nullable EntityUptime defaultUptime) {
        this.cloudScopeStore = Objects.requireNonNull(cloudScopeStore);
        this.defaultUptime = defaultUptime;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void persistTopologyUptime(@Nonnull final TimeInterval uptimeWindow,
                                      @Nonnull final Map<Long, EntityUptime> entityUptimeMap) {

        uptimeDataLock.writeLock().lock();
        try {
            this.uptimeWindow = uptimeWindow;
            this.entityUptimeMap = ImmutableMap.copyOf(entityUptimeMap);
        } finally {
            uptimeDataLock.writeLock().unlock();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public TimeInterval getUptimeWindow() {
        uptimeDataLock.readLock().lock();
        try {
            return uptimeWindow;
        } finally {
            uptimeDataLock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Map<Long, EntityUptime> getUptimeByFilter(@Nonnull final CloudScopeFilter filter) {
        uptimeDataLock.readLock().lock();
        try {
            // The use of the cloud scope store, in order ot determine entities in scope of
            // the filter, is dependent on entity uptime being calculated based on CCA data. This guarantees
            // if there is an entity uptime entry for an entity, it must also have an entry in
            // the cloud scope store.
            final Map<Long, EntityUptime> map = new HashMap<>();
            cloudScopeStore.streamByFilter(filter, entityCloudScope -> {
                long oid = entityCloudScope.entityOid();
                if (entityUptimeMap.containsKey(oid)) {
                    map.put(oid, entityUptimeMap.get(oid));
                }
            });
            return ImmutableMap.copyOf(map);
        } finally {
            uptimeDataLock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Optional<EntityUptime> getEntityUptime(final long entityOid) {
        uptimeDataLock.readLock().lock();
        try {
            return Optional.ofNullable(entityUptimeMap.get(entityOid));
        } finally {
            uptimeDataLock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Optional<EntityUptime> getDefaultUptime() {
        return Optional.ofNullable(defaultUptime);
    }

    @Nonnull
    @Override
    public Map<Long, EntityUptime> getAllEntityUptime() {
        uptimeDataLock.readLock().lock();
        try {
            return ImmutableMap.copyOf(entityUptimeMap);
        } finally {
            uptimeDataLock.readLock().unlock();
        }
    }
}
