package com.vmturbo.action.orchestrator.api;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification.EntitiesWithSeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification.SeverityBreakdown;

/**
 * Listener for changes in {@link EntitySeverityClientCache}.
 * Clients should implement this to execute any kind of logic that requires that the
 * {@link EntitySeverityClientCache} has been updated after entities' severity changes.
 */
public interface EntitySeverityClientCacheUpdateListener {

    /**
     * Cache has been fully refreshed (a new action plan has been received).
     */
    default void onEntitySeverityClientCacheRefresh() { }

    /**
     * Cache has been partially refreshed (an action was executed).
     *
     * @param newSeveritiesByEntity Map from entity oid to the entity's new severity. This map
     *                              may contain {@link Severity#NORMAL}, since an entity that was
     *                              previously non-normal may now become normal.
     * @param updatedBreakdowns Updates to the map from entity OID to the {@link SeverityBreakdown}
     *                          for that entity (for ARM entities).
     */
    default void onEntitySeverityClientCacheUpdate(
            @Nonnull Collection<EntitiesWithSeverity> newSeveritiesByEntity,
            @Nonnull Map<Long, SeverityBreakdown> updatedBreakdowns) { }
}
