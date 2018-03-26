package com.vmturbo.action.orchestrator.action;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
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

    /**
     * Filter actions. If filter has startDate and endDate, it will only retrieve historical actions
     * in that range.
     *
     * @param actionStore action store
     * @return stream of actions that pass the filter
     */
    public Stream<ActionView> filteredActionViews(@Nonnull final ActionStore actionStore) {
        if (optionalFilter.isPresent() && optionalFilter.get().hasStartDate()
                && optionalFilter.get().hasEndDate()) {
            LocalDateTime startDate = getLocalDateTime(optionalFilter.get().getStartDate());
            LocalDateTime endDate = getLocalDateTime(optionalFilter.get().getEndDate());
            return actionStore.getActionViewsByDate(startDate, endDate).values().stream()
                    .filter(actionView -> test(actionView, actionStore.getVisibilityPredicate()));
        } else {
            return actionStore.getActionViews().values().stream()
                    .filter(actionView -> test(actionView, actionStore.getVisibilityPredicate()));
        }
    }

    /**
     * Group the actions by recommended date.
     *
     * Group action type count by action recommendation date.
     * If there is no involved actions within provided period, return empty map.
     *
     * @param actionStore action store
     * @return Map (key, value) -> (recommended date, set of actions)
     */
    public Map<Long, List<ActionView>> filteredActionViewsGroupByDate(@Nonnull final ActionStore actionStore) {
        if (optionalFilter.isPresent() && optionalFilter.get().hasStartDate()
                && optionalFilter.get().hasEndDate()) {
            LocalDateTime startDate = getLocalDateTime(optionalFilter.get().getStartDate());
            LocalDateTime endDate = getLocalDateTime(optionalFilter.get().getEndDate());
            Stream<ActionView> actionViewStream =  actionStore.getActionViewsByDate(startDate, endDate)
                    .values()
                    .stream()
                    .filter(actionView -> test(actionView, actionStore.getVisibilityPredicate()));
            return actionViewStream.collect(Collectors.groupingBy(action ->
                    localDateTimeToDate(action
                            .getRecommendationTime()
                            .toLocalDate()
                            .atStartOfDay()))); // Group by start of the Day
        }
        return Maps.newHashMap();
    }

    /**
     * Convert date time to local date time.
     *
     * @param dateTime date time with long type.
     * @return local date time with LocalDateTime type.
     */
    private LocalDateTime getLocalDateTime(long dateTime) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTime),
                                    TimeZone.getDefault().toZoneId());
    }

    /**
     * Convert local date time to long.
     * @param startOfDay start of date with LocalDateTime type.
     * @return date time in long type.
     */
    private long localDateTimeToDate(LocalDateTime startOfDay) {
        return Date.from(startOfDay.atZone(ZoneId.systemDefault()).toInstant()).getTime();
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

        // Same as states check, we also check the mode too.
        if (!filter.getModesList().isEmpty() &&
                !filter.getModesList().contains(actionView.getMode())) {
            return false;
        }

        if (!filter.getTypesList().isEmpty() &&
                !filter.getTypesList().contains(
                        ActionDTOUtil.getActionInfoActionType(actionView.getRecommendation()))) {
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

        if (!filter.getCategoriesList().isEmpty() && !filter.getCategoriesList().contains(
                // This uses the pre-translation explanation to assign the category, whereas the
                // final category extraction is done post-translation. At the time of this writing
                // (March 26, 2018) no translation operations affect the category of explanations,
                // only the contents (e.g. units).
                ActionCategoryExtractor.assignActionCategory(
                        actionView.getRecommendation().getExplanation()))) {
            return false;
        }

        return true;
    }
}
