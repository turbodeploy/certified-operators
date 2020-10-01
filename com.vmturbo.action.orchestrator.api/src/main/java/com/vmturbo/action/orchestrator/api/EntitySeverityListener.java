package com.vmturbo.action.orchestrator.api;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification.EntitiesWithSeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification.SeverityBreakdown;

/**
 * Listener for severity changes from the action orchestrator.
 */
public interface EntitySeverityListener {

    /**
     * A full refresh of entity severities (triggered by the reception of a new action plan).
     *
     * @param entitiesBySeverity A map from {@link Severity} to entities with that severity.
     *                           This map will NOT contain {@link Severity#NORMAL}, because all
     *                           entities have "normal" severity by default.
     * @param severityBreakdowns The new map from entity OID to the {@link SeverityBreakdown} for that
     *                           entity (for ARM entities).
     */
    void entitySeveritiesRefresh(@Nonnull Collection<EntitiesWithSeverity> entitiesBySeverity,
            @Nonnull Map<Long, SeverityBreakdown> severityBreakdowns);

    /**
     * A partial update of severities for several entities (triggered by the execution of an
     * action).
     *
     * @param newSeveritiesByEntity Map from entity oid to the entity's new severity. This map
     *                              may contain {@link Severity#NORMAL}, since an entity that was
     *                              previously non-normal may now become normal.
     * @param updatedBreakdowns Updates to the map from entity OID to the {@link SeverityBreakdown} for that
     *                           entity (for ARM entities).
     */
    void entitySeveritiesUpdate(@Nonnull Collection<EntitiesWithSeverity> newSeveritiesByEntity,
            @Nonnull Map<Long, SeverityBreakdown> updatedBreakdowns);
}
