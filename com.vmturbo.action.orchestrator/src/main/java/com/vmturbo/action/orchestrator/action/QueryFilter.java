package com.vmturbo.action.orchestrator.action;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

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
