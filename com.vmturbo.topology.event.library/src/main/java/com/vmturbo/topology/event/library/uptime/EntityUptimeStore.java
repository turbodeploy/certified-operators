package com.vmturbo.topology.event.library.uptime;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.common.protobuf.cost.EntityUptime.CloudScopeFilter;

/**
 * A store for {@link EntityUptime}.
 */
public interface EntityUptimeStore {

    /**
     * Persists the {@link EntityUptime} per entity for the provided {@code uptimeWindow}.
     * @param uptimeWindow The uptime window used to calculate entity uptime.
     * @param entityUptimeMap Entity uptime by entity OID.
     */
    void persistTopologyUptime(@Nonnull TimeInterval uptimeWindow,
                               @Nonnull Map<Long, EntityUptime> entityUptimeMap);

    /**
     * The uptime window currently persisted to the store.
     * @return The last persisted uptime window. If no uptime window has been persisted,
     * {@link TimeInterval#EPOCH} will be returned.
     */
    @Nonnull
    TimeInterval getUptimeWindow();

    /**
     * Queries for entity uptime based on the provided {@code filter}.
     * @param filter The {@link CloudScopeFilter}.
     * @return An immutable map of entity uptime by entity OID that meet the provided {@code filter}.
     */
    @Nonnull
    Map<Long, EntityUptime> getUptimeByFilter(@Nonnull CloudScopeFilter filter);

    /**
     * Queries the entity uptime for {@code entityOid}.
     * @param entityOid The target entity OID.
     * @return The entity uptime for {@code entityOid}. If no entity uptime for that OID has been
     * persisted, {@link #getDefaultUptime()} will be returned.
     */
    @Nonnull
    Optional<EntityUptime> getEntityUptime(long entityOid);

    /**
     * The default entity uptime.
     * @return The default entity uptime. May be null, if entity uptime is disabled.
     */
    @Nonnull
    Optional<EntityUptime> getDefaultUptime();
}
