package com.vmturbo.action.orchestrator.stats.query.live;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import io.grpc.Status;

import org.jooq.exception.DataAccessException;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.stats.query.live.CombinedStatsBuckets.CombinedStatsBucketsFactory;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.EntityScope;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.ScopeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest.SingleQuery;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Reader for "current" stats - the stats in a particular {@link ActionStore}. Derives action
 * stats by looking over the {@link Action}s in an {@link ActionStore}.
 * <p>
 * Distinct from {@link com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader}, which
 * reads pre-calculated stats from the action stat tables in the database.
 */
public class CurrentActionStatReader {

    private final QueryInfoFactory queryInfoFactory;

    private final CombinedStatsBucketsFactory statsBucketsFactory;

    private final ActionStorehouse actionStorehouse;

    public CurrentActionStatReader(final long realtimeContextId,
                                   @Nonnull final ActionStorehouse actionStorehouse) {
        this(new QueryInfoFactory(realtimeContextId),
            new CombinedStatsBucketsFactory(),
            actionStorehouse);
    }

    /**
     * Constructor for testing purposes to inject additional mocks.
     */
    @VisibleForTesting
    CurrentActionStatReader(@Nonnull final QueryInfoFactory queryInfoFactory,
                            @Nonnull final CombinedStatsBucketsFactory statsBucketsFactory,
                            @Nonnull final ActionStorehouse actionStorehouse) {
        this.queryInfoFactory = Objects.requireNonNull(queryInfoFactory);
        this.statsBucketsFactory = Objects.requireNonNull(statsBucketsFactory);
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
    }

    /**
     * Read a set of action stats from the {@link ActionStore}s they target.
     * Does one pass through the actions in each {@link ActionStore} targetted by the input
     * queries.
     *
     * @param actionStatsRequest The {@link GetCurrentActionStatsRequest}.
     * @return A map of (query id) -> (list of {@link CurrentActionStat}s for the query).
     */
    @Nonnull
    public Map<Long, List<CurrentActionStat>> readActionStats(@Nonnull final GetCurrentActionStatsRequest actionStatsRequest)
            throws FailedActionQueryException {
        Metrics.CUR_QUERIES_GAUGE.increment();
        try {
            // Since we want to return an entry (even if empty) for every single query in the
            // request, initialize the response map.
            final Map<Long, List<CurrentActionStat>> responseByQueryId =
                actionStatsRequest.getQueriesList().stream()
                    // If multiple queries share the same ID, we throw an exception back to the
                    // caller.
                    .collect(Collectors.toMap(SingleQuery::getQueryId, k -> new ArrayList<>()));

            Metrics.QUERIES_IN_REQUEST.observe((double)responseByQueryId.size());

            // Index queries by context ID, so we know which queries to route to which action stores.
            final Multimap<Long, QueryInfo> queriesByContextId =
                Multimaps.index(actionStatsRequest.getQueriesList().stream()
                    .map(queryInfoFactory::extractQueryInfo)
                    .iterator(), QueryInfo::topologyContextId);

            for (final Entry<Long, Collection<QueryInfo>> x : queriesByContextId.asMap().entrySet()) {
                final long contextId = x.getKey();
                final Collection<QueryInfo> queriesForContext = x.getValue();
                final Optional<ActionStore> storeForContext = actionStorehouse.getStore(contextId);
                if (storeForContext.isPresent()) {
                    final ActionStore store = storeForContext.get();
                    try (DataMetricTimer timer = Metrics.QUERY_DURATION_SUMMARY
                            .labels(store.getStoreTypeName())
                            .startTimer()) {
                        responseByQueryId.putAll(queryActionStore(storeForContext.get(), queriesForContext));
                    } catch (DataAccessException e) {
                        // We may hit this exception with an action store that interacts with the
                        // database.
                        //
                        // We could continue with the other action stores, but realistically we
                        // don't expect queries that mix database-backed and live action stores,
                        // so there's no need to over-complicate the interface for now.
                        throw new FailedActionQueryException(Status.INTERNAL,
                            "Failed to retrieve actions for context: " + contextId
                                + ". Error: " + e.getMessage(), e);
                    }
                } else {
                    // As above - we could continue with the other action stores, but realistically
                    // we don't expect queries for multiple action stores (e.g. a single query for
                    // both plan and live), so we don't deal with it for now.
                    throw new FailedActionQueryException(Status.NOT_FOUND,
                        "No action store found for context: " + contextId);
                }
            }

            return responseByQueryId;
        } finally {
            Metrics.CUR_QUERIES_GAUGE.decrement();
        }
    }

    @Nonnull
    private Map<Long, List<CurrentActionStat>> queryActionStore(
                @Nonnull final ActionStore actionStore,
                @Nonnull final Collection<QueryInfo> queries) {
        final Map<Long, CombinedStatsBuckets> bucketsByQueryId = queries.stream()
            .collect(Collectors.toMap(
                QueryInfo::queryId,
                statsBucketsFactory::bucketsForQuery));

        final QueryableActionViews actionViews = actionStore.getActionViews();

        final Map<ScopeFilter.ScopeCase, List<QueryInfo>> queriesByScopeCase = queries.stream()
            .collect(Collectors.groupingBy(query -> query.query().getScopeFilter().getScopeCase()));

        final Stream<ActionView> candidateActionViews;
        // If requested scope for all queries is based on entity IDs then get action views for
        // requested entities. Otherwise get all action views.
        if (queriesByScopeCase.containsKey(ScopeCase.ENTITY_LIST)
                && queriesByScopeCase.keySet().size() == 1) {
            // collect all involved entities into a set for all scopes in the queries first,
            // so actions are already deduplicated in getByEntity before returning
            Set<Long> allInvolvedEntities = queriesByScopeCase.get(ScopeCase.ENTITY_LIST).stream()
                .flatMap(queryInfo ->
                    queryInfo.query().getScopeFilter().getEntityList().getOidsList().stream())
                .collect(Collectors.toSet());
            candidateActionViews = actionViews.getByEntity(allInvolvedEntities);
        } else {
            candidateActionViews = actionViews.getAll();
        }

        candidateActionViews
            .map(actionView -> {
                try {
                    // The main reason to construct this helper is to pre-calculate the involved
                    // entities, since we need them often.
                    return Optional.of(ImmutableSingleActionInfo.builder()
                        .action(actionView)
                        .involvedEntities(Sets.newHashSet(ActionDTOUtil.getInvolvedEntities(
                                actionView.getTranslationResultOrOriginal())))
                        .build());
                } catch (UnsupportedActionException e) {
                    return Optional.<SingleActionInfo>empty();
                }
            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(singleAction -> {
                queries.forEach(queryInfo -> {
                    if (queryInfo.viewPredicate().test(singleAction)) {
                        bucketsByQueryId.get(queryInfo.queryId())
                            .addActionInfo(singleAction);
                    }
                });
            });

        return bucketsByQueryId.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey,
                entry -> entry.getValue().toActionStats().collect(Collectors.toList())));
    }

    static class Metrics {
        private static final DataMetricGauge CUR_QUERIES_GAUGE = DataMetricGauge.builder()
            .withName("ao_cur_action_stat_queries")
            .withHelp("Number in-progress queries for current action stats.")
            .build()
            .register();

        private static final DataMetricHistogram QUERIES_IN_REQUEST = DataMetricHistogram.builder()
            .withName("ao_cur_action_stat_queries_in_request")
            .withHelp("Number of single queries in a current action stats request.")
            .withBuckets(2, 10, 100)
            .build();

        private static final DataMetricSummary QUERY_DURATION_SUMMARY = DataMetricSummary.builder()
            .withName("ao_cur_action_stat_query_duration_seconds")
            .withHelp("Duration of action stats queries.")
            .withLabelNames("store_type")
            .build();
    }
}
