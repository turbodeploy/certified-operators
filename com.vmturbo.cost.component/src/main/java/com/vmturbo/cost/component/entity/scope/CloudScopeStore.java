package com.vmturbo.cost.component.entity.scope;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.diagnostics.DiagsRestorable;

/**
 * A store for {@link EntityCloudScope} records. The store is used to consolidate scoping information
 * for a cloud entity, containing immutable attributes of an entity, which are currently not persisted
 * when the entity is removed from the topology. This store allows other dependent stores to track
 * historical entity data and support queries based on a cloud scope.
 */
public interface CloudScopeStore extends DiagsRestorable<Void> {


    /**
     * Cleans up any cloud scope records which are no longer referenced by a child store/table.
     *
     * @return The number of records deleted.
     */
    long cleanupCloudScopeRecords();

    /**
     * Streams all {@link EntityCloudScope} records.
     * @return A {@link Stream} contain all {@link EntityCloudScope} records that currently exist.
     */
    @Nonnull
    Stream<EntityCloudScope> streamAll();
}
