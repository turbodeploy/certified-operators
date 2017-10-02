package com.vmturbo.action.orchestrator.store;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.QueryFilter;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;

/**
 * Maintain a cache of entity severities. Refreshing the entire cache causes the recomputation of the
 * severity for every entity in the cache. Refreshing the cache when a single action changes updates
 * causes the recomputation of only the "SeverityEntity"
 * (see {@link com.vmturbo.common.protobuf.ActionDTOUtil#getSeverityEntity(Action)}
 * for that action (although this operation is not that much faster than a full recomputation because
 * it still requires an examination of every action in the {@link ActionStore}.
 *
 * The severity for an entity is considered the maximum severity across all "visible" actions for
 * which the entity is a "SeverityEntity".
 *
 * The cache should be invalidated and refreshed when:
 * 1. A new action arrives in the system
 * 2. The "visibility" of an existing action in the system changes. (Visibility defined as whether
 * the user can see it in the UI).
 * 3. An action transitions from READY to any other state.
 *
 * TODO: (davidblinn) The work being done here is somewhat specific to the API. Consider
 * moving this responsibility to the API if possible.
 */
@ThreadSafe
public class EntitySeverityCache {
    private final Logger logger = LogManager.getLogger();

    private final Map<Long, Severity> severities = Collections.synchronizedMap(new HashMap<>());

    private final SeverityComparator severityComparator = new SeverityComparator();

    private final QueryFilter visibilityQueryFilter;

    public EntitySeverityCache(@Nonnull final QueryFilter visibilityQueryFilter) {
        this.visibilityQueryFilter = visibilityQueryFilter;
    }

    /**
     * Invalidate and refresh the calculated severity based on the current contents of the action store.
     */
    public void refresh(@Nonnull final ActionStore actionStore) {
        synchronized(severities) {
            severities.clear();

            visibleReadyActionViews(actionStore)
                .forEach(this::handleActionSeverity);
        }
    }

    /**
     * Refresh the calculated severity for the "SeverityEntity" for the given
     * action based on the current contents of the {@link ActionStore}.
     *
     * TODO: (davidblinn) This may need to be made more efficient when lots of actions are updated
     *
     * @param action The action whose "SeverityEntity" should have its severity recalculated.
     */
    public void refresh(@Nonnull final Action action, @Nonnull final ActionStore actionStore) {
        try {
            long severityEntity = ActionDTOUtil.getSeverityEntity(action);

            visibleReadyActionViews(actionStore)
                .filter(actionView -> matchingSeverityEntity(severityEntity, actionView))
                .map(actionView ->
                    ActionDTOUtil.mapImportanceToSeverity(actionView.getRecommendation().getImportance()))
                .max(severityComparator)
                .ifPresent(severity -> severities.put(severityEntity, severity));
        } catch (UnsupportedActionException e) {
            logger.error("Unable to refresh severity cache for action {}", action, e);
        }
    }

    /**
     * Get the severity for a given entity by that entity's OID.
     *
     * @param entityOid The OID of the entity whose severity should be retrieved.
     * @return The severity  of the entity. Optional.empty() if the severity of the entity is unknown.
     */
    @Nonnull
    public Optional<Severity> getSeverity(long entityOid) {
        return Optional.ofNullable(severities.get(entityOid));
    }

    /**
     * Set the cached value for the actionView's severityEntity to be the maximum of
     * the current value for the severityEntity and the severity associated with the actionView.
     *
     * @param actionView The action whose severity should be updated.
     */
    private void handleActionSeverity(@Nonnull final ActionView actionView) {
        try {
            final long severityEntity = ActionDTOUtil.getSeverityEntity(actionView.getRecommendation());
            final Severity nextSeverity = ActionDTOUtil.mapImportanceToSeverity(
                actionView.getRecommendation().getImportance());

            final Severity maxSeverity = getSeverity(severityEntity)
                .map(currentSeverity -> maxSeverity(currentSeverity, nextSeverity))
                .orElse(nextSeverity);

            severities.put(severityEntity, maxSeverity);
        } catch (UnsupportedActionException e) {
            logger.warn("Unable to handle action severity for action {}", actionView);
        }
    }

    /**
     * Check if the severity entity for the ActionView matches the input severityEntity.
     *
     * @param severityEntity The oid of the severityEntity to check for matches.
     * @param actionView The actionView to check as a match.
     * @return True if the severityEntity for the actionView matches the input severityEntity.
     *         False if there is no match or the severity entity for the spec cannot be determined.
     */
    private boolean matchingSeverityEntity(long severityEntity, @Nonnull final ActionView actionView) {
        try {
            long specSeverityEntity = ActionDTOUtil.getSeverityEntity(actionView.getRecommendation());
            return specSeverityEntity == severityEntity;
        } catch (UnsupportedActionException e) {
            return false;
        }
    }

    private Stream<ActionView> visibleReadyActionViews(@Nonnull final ActionStore actionStore) {
        return visibilityQueryFilter.filteredActionViews(actionStore)
            .filter(actionView -> actionView.getState() == ActionState.READY);
    }

    /**
     * Compare severities, returning the more severe {@link Severity}.
     *
     * @param s1 The first severity
     * @param s2 The second severity
     * @return The severity that is the more severe. In the case where they are equally
     *         severe no guarantee is made about which will be returned.
     */
    @Nonnull
    private Severity maxSeverity(Severity s1, Severity s2) {
        return severityComparator.compare(s1, s2) > 0 ? s1 : s2;
    }

    /**
     * Compare severities. Higher severities are ordered before lower severities.
     */
    private static class SeverityComparator implements Comparator<Severity> {

        @Override
        public int compare(Severity s1, Severity s2) {
            return s1.getNumber() - s2.getNumber();
        }
    }
}
