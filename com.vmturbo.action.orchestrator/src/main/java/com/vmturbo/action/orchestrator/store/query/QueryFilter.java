package com.vmturbo.action.orchestrator.store.query;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionEnvironmentType;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;

/**
 * A optionalFilter that can be used to test whether an action passes or fails an
 * {@link ActionQueryFilter}.
 */
public class QueryFilter {
    private final ActionQueryFilter filter;

    // If present, this is a set of the OIDs from the involved entities list in the
    // ActionQueryFilter to support constant-time lookups.
    private final Set<Long> involvedEntities;

    private final Predicate<ActionView> visibilityPredicate;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a new query filter.
     * @param filter The filter to test against. If empty, all actions pass the filter.
     */
    public QueryFilter(@Nonnull final ActionQueryFilter filter,
                       @Nonnull final Predicate<ActionView> visibilityPredicate) {
        this.filter = filter;
        this.visibilityPredicate = visibilityPredicate;
        this.involvedEntities = new HashSet<>(
            filter.getInvolvedEntities().getOidsList());
    }

    /**
     * Test whether an action passes the filter.
     *
     * @param actionView The action to test against the filter.
     * @return true if the action passes the filter, false if the action does not pass the filter.
     */
    public boolean test(@Nonnull final ActionView actionView) {
        return test(filter, actionView);
    }

    private boolean test(@Nonnull final ActionQueryFilter filter,
                         @Nonnull final ActionView actionView) {
        if (filter.hasVisible() && filter.getVisible() != visibilityPredicate.test(actionView)) {
            return false;
        }

        // Using List.contains is ok, because the number of acceptable
        // states should be small (less than the total states).
        if (!filter.getStatesList().isEmpty() &&
            !filter.getStatesList().contains(actionView.getState())) {
            return false;
        }

        // Same as states check, we also check the mode too.
        if (!filter.getModesList().isEmpty() &&
                !filter.getModesList().contains(actionView.getMode())) {
            return false;
        }

        if (!filter.getTypesList().isEmpty() &&
                !filter.getTypesList().contains(ActionDTOUtil.getActionInfoActionType(
                        actionView.getTranslationResultOrOriginal()))) {
            return false;
        }

        if (filter.hasEnvironmentType()) {
            try {
                final ActionEnvironmentType envType =
                    ActionEnvironmentType.forAction(actionView.getTranslationResultOrOriginal());
                if (!envType.matchesEnvType(filter.getEnvironmentType())) {
                    return false;
                }
            } catch (UnsupportedActionException e) {
                logger.error("Unsupported action found in action store: {}", e.getMessage());
                return false;
            }
        }


        // Return false if the action is not related to the specified entities.
        if (filter.hasInvolvedEntities() || filter.getEntityTypeCount() > 0) {
            try {
                // Get the involved entity once.
                List<ActionEntity> actionInvolvedEntities = ActionDTOUtil.getInvolvedEntities(
                    actionView.getTranslationResultOrOriginal());
                // If the caller specified an explicit list of OID to look for, we look in that
                // list and ignore the specified list of entity types.
                if (filter.getInvolvedEntities().getOidsCount() > 0) {
                    final boolean containsInvolved = actionInvolvedEntities.stream()
                        .anyMatch(actionInvolvedEntity -> this.involvedEntities.contains(actionInvolvedEntity.getId()));
                    if (!containsInvolved) {
                        return false;
                    }
                } else if (filter.hasInvolvedEntities() && actionInvolvedEntities.isEmpty()) {
                    // If the filter is explicitly looking for actions that involve no entities,
                    // and this action has no involved entities (e.g. buy RI) then this action
                    // matches the filter.
                    return true;
                } else if (filter.hasInvolvedEntities()) {
                    // If the filter has an explicitly set "InvolvedEntities" message containing
                    // nothing, then no actions match.
                    return false;
                }

                if (filter.getEntityTypeCount() > 0) {
                    // If the caller DID NOT specify an explicit list of OIDs, but DID specify a
                    // list of entity types, check the involved entities to see if they match the
                    // types.
                    final boolean containsType = actionInvolvedEntities.stream()
                        .anyMatch(actionInvolvedEntity ->
                            // This is a "contains" on a list, but the size of the list will be
                            // small.
                            filter.getEntityTypeList()
                                .contains(actionInvolvedEntity.getType()));
                    if (!containsType) {
                        return false;
                    }
                }
            } catch (UnsupportedActionException e) {
                return false;
            }
        }

        if (!filter.getCategoriesList().isEmpty() &&
                !filter.getCategoriesList().contains(actionView.getActionCategory())) {
            return false;
        }

        return true;
    }
}
