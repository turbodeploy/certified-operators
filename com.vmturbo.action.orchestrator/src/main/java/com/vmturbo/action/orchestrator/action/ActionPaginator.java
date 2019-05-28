package com.vmturbo.action.orchestrator.action;

import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.action.orchestrator.store.query.QueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.ActionOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;

/**
 * The {@link ActionPaginator} is responsible for applying {@link PaginationParameters} to a
 * stream of {@link ActionView}s.
 * <p>
 * At the time of this writing (5 April 2018) all filtering is done in-memory by the
 * {@link QueryFilter}, for live and plan, and current and historical actions. Since pagination
 * has to happen after filtering, this means pagination has to be done in memory as well. In
 * the future we may want to handle filtering - at least for plan and historical actions - in
 * the database, and if that's the case we should handle pagination there too.
 */
public class ActionPaginator {

    private static final Logger logger = LogManager.getLogger();

    private static final ActionOrderBy DEFAULT_ORDER_BY = ActionOrderBy.ACTION_SEVERITY;

    /**
     * The {@link Comparator}s that will handle the ordering of action views.
     */
    private static final Map<ActionOrderBy, Comparator<ActionView>> COMPARATOR_REGISTRY;

    static {
        final Map<ActionOrderBy, Comparator<ActionView>> registry = new EnumMap<>(ActionOrderBy.class);
        // TODO (roman, 5 April 2018): The action types in ActionDTO.ActionType are not completely
        // in sync with the action types we return to the API. Using these types for the sort
        // order means there may be some inconsistencies - i.e. two action with the same
        // ActionDTO.ActionType will be treated as equivalent for sorting purposes even if they are
        // mapped to two different types in the API component.
        //
        // We need to have a single source-of-truth for action types, and it needs to be the
        // action orchestrator for pagination to work properly.
        registry.put(ActionOrderBy.ACTION_TYPE, Comparator.comparing(actionView ->
                        ActionDTOUtil.getActionInfoActionType(actionView.getRecommendation())));
        registry.put(ActionOrderBy.ACTION_SEVERITY,
                Comparator.comparingDouble(view -> view.getRecommendation().getImportance()));
        registry.put(ActionOrderBy.ACTION_RISK_CATEGORY, (a1, a2) -> {
            final ActionCategory a1Category = a1.getActionCategory();
            final ActionCategory a2Category = a2.getActionCategory();
            return a1Category.compareTo(a2Category);
        });
        // We don't have savings/cost implemented for actions in XL, so all actions
        // are the same in this regard.
        registry.put(ActionOrderBy.ACTION_SAVINGS, (a1, a2) -> 0);

        COMPARATOR_REGISTRY = Collections.unmodifiableMap(registry);
    }

    private final int defaultPaginationLimit;

    private final int maxPaginationLimit;

    /**
     * Use {@link DefaultActionPaginatorFactory}.
     */
    private ActionPaginator(final int defaultPaginationLimit,
                            final int maxPaginationLimit) {
        Preconditions.checkArgument(defaultPaginationLimit <= maxPaginationLimit);
        this.defaultPaginationLimit = defaultPaginationLimit;
        this.maxPaginationLimit = maxPaginationLimit;
    }

    /**
     * Filter actions. If filter has startDate and endDate, it will only retrieve historical actions
     * in that range.
     *
     * @param actionViews Stream of actions. It will be closed as part of executing this method.
     * @param paginationParameters Parameters to use to paginate the result.
     * @return A {@link PaginatedActionViews} object containing the actions that pass the
     *         filter, with the pagination parameters applied.
     */
    @Nonnull
    public PaginatedActionViews applyPagination(@Nonnull final Stream<ActionView> actionViews,
                                                @Nonnull final PaginationParameters paginationParameters) {
        final long skipCount;
        if (paginationParameters.hasCursor()) {
            try {
                skipCount = Long.parseLong(paginationParameters.getCursor());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Cursor " + paginationParameters.getCursor() +
                        " is invalid. Should be a number.");
            }
        } else {
            skipCount = 0;
        }

        final long limit;
        if (paginationParameters.hasLimit()) {
            if (paginationParameters.getLimit() > maxPaginationLimit) {
                logger.warn("Client-requested limit {} exceeds maximum!" +
                        " Lowering the limit to {}!", paginationParameters.getLimit(), maxPaginationLimit);
                limit = maxPaginationLimit;
            } else if (paginationParameters.getLimit() > 0) {
                limit = paginationParameters.getLimit();
            } else {
                throw new IllegalArgumentException("Illegal pagination limit: " +
                        paginationParameters.getLimit() + ". Must be be a positive integer");
            }
        } else {
            limit = defaultPaginationLimit;
        }

        final Comparator<ActionView> comparator = getComparator(paginationParameters.getOrderBy());

        // Apply pagination parameters after sorting.
        // Unfortunately, because we do filtering here instead of in the individual action stores
        // we can't apply pagination parameters at the action store yet.
        //
        // It's better to do the sort + limit + offset calculation after filtering, so that we're
        // working with a (potentially) smaller data set.
        final List<ActionView> results = actionViews
                // Sort according to sort parameter
                .sorted(paginationParameters.getAscending() ? comparator : comparator.reversed())
                .skip(skipCount)
                // Add 1 so we know if there are more results or not.
                .limit(limit + 1)
                .collect(Collectors.toList());
        if (results.size() > limit) {
            final String nextCursor = Long.toString(skipCount + limit);
            // Remove the last element to conform to limit boundaries.
            results.remove(results.size() - 1);
            return new PaginatedActionViews(results, Optional.of(nextCursor));
        } else {
            // No more results.
            return new PaginatedActionViews(results, Optional.empty());
        }
    }

    @Nonnull
    private static Comparator<ActionView> getComparator(@Nonnull final OrderBy orderBy) {
        if (orderBy.hasAction()) {
            Comparator<ActionView> result = COMPARATOR_REGISTRY.get(orderBy.getAction());
            if (result == null) {
                logger.error("Unhandled sort order: {}. Using default order: {}",
                        orderBy.getAction(), DEFAULT_ORDER_BY);
                return COMPARATOR_REGISTRY.get(DEFAULT_ORDER_BY);
            }
            return result;
        } else {
            logger.warn("No action order specified. Using default order: {}", DEFAULT_ORDER_BY);
            return COMPARATOR_REGISTRY.get(DEFAULT_ORDER_BY);
        }
    }

    /**
     * Interface for dependency-injection of the paginator, and tests.
     */
    @FunctionalInterface
    public interface ActionPaginatorFactory {
        @Nonnull
        ActionPaginator newPaginator();
    }

    /**
     * The default implementation of {@link ActionPaginatorFactory}, intended for production use.
     */
    public static class DefaultActionPaginatorFactory implements ActionPaginatorFactory {

        private final int defaultPaginationLimit;
        private final int maxPaginationLimit;

        public DefaultActionPaginatorFactory(final int defaultPaginationLimit,
                                             final int maxPaginationLimit) {
            Preconditions.checkArgument(defaultPaginationLimit <= maxPaginationLimit);
            this.defaultPaginationLimit = defaultPaginationLimit;
            this.maxPaginationLimit = maxPaginationLimit;
        }

        @Nonnull
        @Override
        public ActionPaginator newPaginator() {
            return new ActionPaginator(defaultPaginationLimit, maxPaginationLimit);
        }
    }

    /**
     * The result of requesting a set of action views that are both filtered and paginated.
     */
    public static class PaginatedActionViews {
        private final List<ActionView> views;
        private final Optional<String> nextCursor;

        private PaginatedActionViews(@Nonnull final List<ActionView> views,
                                     @Nonnull Optional<String> nextCursor) {
            this.views = views;
            this.nextCursor = nextCursor;
        }

        @Nonnull
        public Optional<String> getNextCursor() {
            return nextCursor;
        }

        @Nonnull
        public List<ActionView> getResults() {
            return views;
        }
    }
}
