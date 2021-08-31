package com.vmturbo.cloud.common.entity.scope;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.EntityUptime.CloudScopeFilter;

/**
 * A store for {@link EntityCloudScope} records. The store is used to consolidate scoping information
 * for a cloud entity, containing immutable attributes of an entity, which are currently not persisted
 * when the entity is removed from the topology. This store allows other dependent stores to track
 * historical entity data and support queries based on a cloud scope.
 */
public interface CloudScopeStore {


    /**
     * Cleans up any cloud scope records which are no longer referenced by a child store/table.
     *
     * @return The number of records deleted.
     */
    long cleanupCloudScopeRecords();

    /**
     * Streams all {@link EntityCloudScope} records.
     *
     * <p>NOTE: This stream should be treated as a closeable resource, as it may represent underlying
     * database connections.
     * @return A {@link Stream} contain all {@link EntityCloudScope} records that currently exist.
     */
    @Nonnull
    Stream<EntityCloudScope> streamAll();

    /**
     * Streams all {@link EntityCloudScope} passing {@code filter}.
     *
     * <p>NOTE: This stream should be treated as a closeable resource, as it may represent underlying
     * database connections.
     * @param filter The {@link CloudScopeFilter}.
     * @return A {@link Stream} containing all {@link EntityCloudScope} records that pass the filter.
     */
    Stream<EntityCloudScope> streamByFilter(@Nonnull CloudScopeFilter filter);
}
