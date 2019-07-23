package com.vmturbo.api.component.external.api.util.stats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.service.TargetsService;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.api.utils.StatsUtils;

/**
 * A shared utility class to execute stats queries, meant to be used by whichever
 * service needs to do so.
 * <p>
 * Responsible for executing stats queries, breaking them up into sub-queries that go to the various
 * components that own the data, and combining the results.
 */
public class StatsQueryExecutor {

    private static final Logger logger = LogManager.getLogger();

    private final StatsQueryContextFactory contextFactory;

    private final StatsQueryScopeExpander scopeExpander;

    private final Set<StatsSubQuery> queries = new HashSet<>();

    public StatsQueryExecutor(@Nonnull final StatsQueryContextFactory contextFactory,
                              @Nonnull final StatsQueryScopeExpander scopeExpander) {
        this.contextFactory = contextFactory;
        this.scopeExpander = scopeExpander;
    }

    /**
     * Register a sub-query with the executor. THIS SHOULD ONLY BE USED IN SPRING CONFIGURATIONS!
     *
     * Note - the only reason we use this method instead of injecting the sub-queries in the constructor
     * is to avoid Spring circular dependencies. The various sub-queries can have a lot of dependencies,
     * and if the {@link StatsQueryExecutor} consumes them directly then we dramatically increase
     * the chances of circular imports in the context. We could fix this by using Autowired, but
     * decided that it's cleaner to explicitly inject the queries outside the constructor.
     *
     * @param query The query to add.
     */
    public void addSubquery(@Nonnull final StatsSubQuery query) {
        this.queries.add(query);
    }

    /**
     * Get a set of aggregated stats for a particular scope.
     *
     * "Aggregated" means that all entities in the scope are rolled into a single value for each
     * (snapshot time, stat name) tuple.
     *
     * This call may result in multiple sub-queries depending on the requested stats.
     *
     * @param scope The scope for the query.
     * @param inputDTO Describes the target time range and stats.
     * @return A list of {@link StatSnapshotApiDTO}, one for every snapshot in the requested
     *         time range.
     * @throws OperationFailedException If there is a critical error.
     */
    @Nonnull
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final ApiId scope,
                                                      @Nonnull final StatPeriodApiInputDTO inputDTO) throws OperationFailedException {
        final StatsQueryScope expandedScope = scopeExpander.expandScope(scope, inputDTO.getStatistics());

        // Check if there is anything in the scope.
        if (!expandedScope.isAll() && expandedScope.getEntities().isEmpty()) {
            return Collections.emptyList();
        }

        final StatsQueryContext context =
            contextFactory.newContext(scope, expandedScope, inputDTO);

        final Map<StatsSubQuery, SubQueryInput> inputsByQuery = assignInputToQueries(context);

        final Table<Long, String, StatApiDTO> statsByDateAndName = HashBasedTable.create();
        // Run the individual queries and assemble their results.
        for (Entry<StatsSubQuery, SubQueryInput> entry : inputsByQuery.entrySet()) {
            final StatsSubQuery query = entry.getKey();
            final SubQueryInput subQueryInput = entry.getValue();
            if (subQueryInput.shouldRunQuery()) {
                try {
                    query.getAggregateStats(subQueryInput.getRequestedStats(), context).forEach((date, stats) -> {
                        stats.forEach(statApiDTO -> {
                            final StatApiDTO prevValue = statsByDateAndName.put(date,
                                statApiDTO.getName(), statApiDTO);
                            if (prevValue != null) {
                                logger.warn("Sub-query {} returned stat {}," +
                                        " which was already returned by another sub-query for the same time.",
                                    query.getClass().getSimpleName(), statApiDTO.getName());
                            }
                        });
                    });
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == Code.UNAVAILABLE) {
                        // If some component is unavailable we don't want to fail the entire
                        // query.
                        logger.warn("Query: {} failed, because the component was unavailabe: {}",
                            query.getClass().getSimpleName(), e.getStatus().getDescription());
                    } else {
                        throw e;
                    }
                }
            }
        }

        // Sort the stats in ascending order by time.
        final Comparator<Entry<Long, Map<String, StatApiDTO>>> ascendingByTime =
            Comparator.comparingLong(Entry::getKey);
        final List<StatSnapshotApiDTO> stats = statsByDateAndName.rowMap().entrySet().stream()
            .sorted(ascendingByTime)
            .map(entry -> {
                final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                statSnapshotApiDTO.setDate(DateTimeUtil.toString(entry.getKey()));
                statSnapshotApiDTO.setStatistics(new ArrayList<>(entry.getValue().values()));
                return statSnapshotApiDTO;
            })
            .collect(Collectors.toList());
        return StatsUtils.filterStats(stats, context.getTargets().stream()
            .map(thinTarget -> {
                final TargetApiDTO targetApiDTO = new TargetApiDTO();
                targetApiDTO.setCategory(thinTarget.probeInfo().category());
                // We pretend all targets are validated. The stats filtering logic only checks
                // valid targets. It adds extra complexity to keep and
                // maintain the validation status in the target cache, so since targets are
                // generally "valid" we hard-code it here.
                //
                // Note - the intent is also to re-factor stats filtering, to look at the target
                // associated with the scope (instead of the global list of targets).
                targetApiDTO.setStatus(TargetsService.UI_VALIDATED_STATUS);
                return targetApiDTO;
            })
            .collect(Collectors.toList()));
    }

    /**
     * Determine how to divide the requested statistics among the registered sub-queries.
     *
     * @param context The context of the query.
     * @return (sub query) -> ({@link SubQueryInput} for the sub query).
     */
    @Nonnull
    private Map<StatsSubQuery, SubQueryInput> assignInputToQueries(@Nonnull final StatsQueryContext context) {
        // If there are no explicit stat requests, then we are requesting "all."
        //
        // Note - it is theoretically possible for one StatApiInputDTO to request specific stats,
        // while another requests "all" stats with another constraint (e.g. entity type).
        // We don't currently have these cases in the UI, so we're not handling them, and logging
        // a warning.
        final Set<StatApiInputDTO> explicitlyRequestedStats = context.getRequestedStats().stream()
            .filter(stat -> stat.getName() != null)
            .collect(Collectors.toSet());
        if (!explicitlyRequestedStats.isEmpty() &&
                context.getRequestedStats().size() != explicitlyRequestedStats.size()) {
            // If there were explicitly requested stats, AND some with name == null, print a warning.
            // We will basically ignore the ones with name == null!
            logger.warn("Detected mix of explicit stat requests and 'all' stat requests. " +
                "Processing the following:\n{}\nIgnoring the following:\n{}",
                explicitlyRequestedStats, context.getRequestedStats().stream()
                    .filter(stat -> !explicitlyRequestedStats.contains(stat))
                    .collect(Collectors.toSet()));
        }
        final boolean requestAll = explicitlyRequestedStats.isEmpty();
        final Map<StatsSubQuery, SubQueryInput> queriesToStats;

        final Set<StatsSubQuery> applicableQueries = queries.stream()
            .filter(query -> query.applicableInContext(context))
            .collect(Collectors.toSet());

        if (requestAll) {
            // Request all stats from all applicable sub-queries.
            final SubQueryInput input = SubQueryInput.all();
            queriesToStats = applicableQueries.stream()
                .collect(Collectors.toMap(Function.identity(), q -> input));
        } else {
            queriesToStats = new HashMap<>();
            // If "leftovers" gets to empty, we need to ignore that query!
            final SubQueryInput leftovers = SubQueryInput.stats(explicitlyRequestedStats);
            applicableQueries.forEach(query -> {
                final SubQuerySupportedStats statsHandledByQuery = query.getHandledStats(context);
                if (statsHandledByQuery.supportsLeftovers()) {
                    queriesToStats.put(query, leftovers);
                } else {
                    Set<StatApiInputDTO> supportedByQuery = Collections.emptySet();
                    final Iterator<StatApiInputDTO> stats = leftovers.getRequestedStats().iterator();
                    while (stats.hasNext()) {
                        final StatApiInputDTO next = stats.next();
                        if (statsHandledByQuery.containsExplicitStat(next)) {
                            if (supportedByQuery.isEmpty()) {
                                supportedByQuery = new HashSet<>();
                            }
                            supportedByQuery.add(next);
                            // This will remove it from "leftovers".
                            stats.remove();
                        }
                    }
                    // We don't check for empty, because we will check for empty later when actually
                    // execute the queries.
                    queriesToStats.put(query, SubQueryInput.stats(supportedByQuery));
                }
            });
        }

        return queriesToStats;
    }

    /**
     * Utility class to determine whether a particular sub-query should run, and, if so,
     * which stats should be requested from it.
     */
    public static class SubQueryInput {

        private final boolean requestAll;

        private final Set<StatApiInputDTO> requestedStats;

        /**
         * Do not use directly! Use {@link SubQueryInput#all()} or {@link SubQueryInput#stats(Set)}.
         */
        private SubQueryInput(final boolean requestAll,
                             final Set<StatApiInputDTO> requestedStats) {
            this.requestAll = requestAll;
            this.requestedStats = requestedStats;
        }

        /**
         * Whether or not the sub-query needs to execute.
         */
        boolean shouldRunQuery() {
            return requestAll || !requestedStats.isEmpty();
        }

        /**
         * The explicitly requested stats.
         *
         * If {@link SubQueryInput#shouldRunQuery()} is true, and {@link SubQueryInput#getRequestedStats()}
         * is empty, then the subquery should return all available stats.
         */
        @Nonnull
        Set<StatApiInputDTO> getRequestedStats() {
            return requestedStats;
        }

        @Nonnull
        public static SubQueryInput all() {
            return new SubQueryInput(true, Collections.emptySet());
        }

        @Nonnull
        public static SubQueryInput stats(@Nonnull final Set<StatApiInputDTO> requestedStats) {
            return new SubQueryInput(false, requestedStats);
        }
    }
}
