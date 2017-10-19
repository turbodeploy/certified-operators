package com.vmturbo.action.orchestrator.action;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;

/**
 * A optionalFilter that can be used to test whether an action passes or fails the optionalFilter.
 */
public class QueryFilter {
    private final Optional<ActionQueryFilter> optionalFilter;

    // If present, this is a set of the OIDs from the involved entities list in the
    // ActionQueryFilter to support constant-time lookups.
    private final Optional<Set<Long>> involvedEntities;

    private final Logger logger = LogManager.getLogger();

    /**
     * A filter for use in testing whether an action is visible to the UI.
     */
    public static final QueryFilter VISIBILITY_FILTER = new QueryFilter(Optional.of(
        ActionQueryFilter.newBuilder().setVisible(true).build()));

    /**
     * Create a new query filter.
     * @param filter The filter to test against. If empty, all actions pass the filter.
     */
    public QueryFilter(@Nonnull final Optional<ActionQueryFilter> filter) {
        this.optionalFilter = filter;

        if (filter.isPresent() && filter.get().hasInvolvedEntities()) {
            this.involvedEntities = Optional.of(new HashSet<Long>(
                            filter.get().getInvolvedEntities().getOidsList()));
        } else {
            this.involvedEntities = Optional.empty();
        }
    }

    public Stream<ActionView> filteredActionViews(@Nonnull final ActionStore actionStore) {
        return actionStore.getActionViews().values().stream()
            .filter(actionView -> test(actionView, actionStore.getVisibilityPredicate()));
    }

    /**
     * Get action views type count by entity Id. When filtering actions, it will use its involved entities.
     * For example: Action 1: Move VM1 from Host1 to Host2, and input entity Id is Host 2. In final Map,
     * it will have an entry: Key is Host2 and Value is Action 1. And Map key will only contains input
     * entity Ids. If there is no involved entities, return empty map.
     *
     * @param actionStore contains all current active actions.
     * @return A map Key is entity id, and value is a list of action views it involves.
     */
    public Multimap<Long, ActionView> filterActionViewsByEntityId(@Nonnull final ActionStore actionStore) {
        final Multimap<Long, ActionView> entityToActionViews = ArrayListMultimap.create();
        // If there is no involved entities, return empty map.
        if (!involvedEntities.isPresent()) {
            return entityToActionViews;
        }
        actionStore.getActionViews().values().stream()
            .filter(actionView -> test(actionView, actionStore.getVisibilityPredicate()))
            .forEach(actionView -> {
                try {
                    final Set<Long> actionInvolvedEntities = ActionDTOUtil.getInvolvedEntities(
                        actionView.getRecommendation());
                    // add actionView to each involved entities entry
                    actionInvolvedEntities.forEach(entityId -> entityToActionViews.put(entityId, actionView));
                } catch (UnsupportedActionException e) {
                    // if action not supported, ignore this action
                    logger.warn("Unsupported action {}", actionView);
                }
        });
        // only keep involved entities
        involvedEntities.ifPresent(entities -> {
            final List<Long> needToRemoveEntityIds = entityToActionViews.keySet().stream()
                .filter(entity -> !entities.contains(entity))
                .collect(Collectors.toList());
            needToRemoveEntityIds.forEach(entityToActionViews::removeAll);
        });
        return entityToActionViews;
    }

    /**
     * Test whether an action passes the filter.
     *
     * @param actionView The action to test against the filter.
     * @return true if the action passes the filter, false if the action does not pass the filter.
     */
    public boolean test(@Nonnull final ActionView actionView,
                        @Nonnull final Predicate<ActionView> visibilityPredicate) {
        return optionalFilter
            .map(filter -> test(filter, actionView, visibilityPredicate))
            .orElse(true);
    }

    private boolean test(@Nonnull final ActionQueryFilter filter,
                         @Nonnull final ActionView actionView,
                         @Nonnull final Predicate<ActionView> visibilityPredicate) {
        if (filter.hasVisible() && filter.getVisible() != visibilityPredicate.test(actionView)) {
            return false;
        }

        // Using List.contains is ok, because the number of acceptable
        // states should be small (less than the total states).
        if (!filter.getStatesList().isEmpty() &&
            !filter.getStatesList().contains(actionView.getState())) {
            return false;
        }

        // Return false if the action is not related to the specified entities.
        if (filter.hasInvolvedEntities()) {
            Set<Long> involvedEntities;
            try {
                involvedEntities = ActionDTOUtil.getInvolvedEntities(
                    actionView.getRecommendation());
            } catch (UnsupportedActionException e) {
                return false;
            }

            if (Sets.intersection(involvedEntities, this.involvedEntities.get()).isEmpty()) {
                return false;
            }
        }

        return true;
    }
}
