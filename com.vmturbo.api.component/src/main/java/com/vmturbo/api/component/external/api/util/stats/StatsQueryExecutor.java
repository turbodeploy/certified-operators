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
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.api.utils.StatsUtils;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

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
     * <p>"Aggregated" means that all entities in the scope are rolled into a single value for each
     * (snapshot time, stat name) tuple.</p>
     *
     * <p>This call may result in multiple sub-queries depending on the requested stats.</p>
     *
     * <p>The results are organized into a collection of {@link StatSnapshotApiDTO}s, each
     * representing a particular stats snapshot time. Within a particular stat snapshot, a
     * particular stat should appear at most once. Results are ordered by snapshot time, ascending.
     * </p>
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
        if (!expandedScope.getGlobalScope().isPresent() && expandedScope.getEntities().isEmpty()
            && !scopeHasBusinessAccounts(scope)) {
            // In case that the scope is a business account / billing family / group of business
            // accounts, the expanded scope will be empty and the input scope will be used directly
            // in CloudCostsStatsSubQuery#getCloudExpensesRecordList.
            // In any other case - return empty stats.
            logger.warn("expandedScope is empty. scopeOID: {}", scope.oid());
            return Collections.emptyList();
        }

        final StatsQueryContext context =
            contextFactory.newContext(scope, expandedScope, inputDTO);

        final Map<StatsSubQuery, SubQueryInput> inputsByQuery = assignInputToQueries(context);

        // table <date, stat identifier, stat>
        final Table<StatTimeAndEpoch, String, StatApiDTO> statsByDateAndId = HashBasedTable.create();
        // Run the individual queries and assemble their results.
        for (Entry<StatsSubQuery, SubQueryInput> entry : inputsByQuery.entrySet()) {
            final StatsSubQuery query = entry.getKey();
            final SubQueryInput subQueryInput = entry.getValue();
            if (subQueryInput.shouldRunQuery()) {
                try {
                    query.getAggregateStats(subQueryInput.getRequestedStats(), context).forEach(statSnapshotApiDTO -> {
                        final long date = DateTimeUtil.parseTime(statSnapshotApiDTO.getDate());
                        final Epoch epoch = statSnapshotApiDTO.getEpoch();
                        statSnapshotApiDTO.getStatistics().forEach(statApiDTO -> {
                            final StatApiDTO prevValue = statsByDateAndId.put(new StatTimeAndEpoch(date, epoch),
                                createStatIdentifier(statApiDTO), statApiDTO);
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
        final Comparator<Entry<StatTimeAndEpoch, Map<String, StatApiDTO>>> ascendingByTime =
            Comparator.comparingLong(entry -> entry.getKey().getTime());
        final List<StatSnapshotApiDTO> stats = statsByDateAndId.rowMap().entrySet().stream()
            .sorted(ascendingByTime)
            .map(entry -> {
                final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                statSnapshotApiDTO.setDate(DateTimeUtil.toString(entry.getKey().getTime()));
                statSnapshotApiDTO.setEpoch(entry.getKey().getEpoch());
                statSnapshotApiDTO.setStatistics(new ArrayList<>(entry.getValue().values()));
                return statSnapshotApiDTO;
            })
            .collect(Collectors.toList());

        // Check if allow showing cooling and power commodities.
        // If not all the entities were discovered by Fabric, then don't allow.
        // The reason that we need to check allowCoolingPower here is:
        // Suppose scope is a mixed group with entities discovered by vCenter and Fabric.
        // Then the target list contains both a vCenter target and a Fabric target.
        // If we send this target list to StatsUtils#filterStats, the returned stats will still
        // contain cooling and power because StatsUtils#allowCoolingPower will always return true.
        final Map<Long, String> targetIdToCategory = context.getTargets().stream().collect(
            Collectors.toMap(ThinTargetInfo::oid, thinTarget -> thinTarget.probeInfo().category()));
        final boolean allowCoolingPower = scope.getDiscoveringTargetIds().stream().allMatch(targetId ->
            ProbeCategory.FABRIC.getCategory().equalsIgnoreCase(targetIdToCategory.get(targetId)));

        // If the request does not contain a start or end time and the response statistics are more
        // than one record, flatten into one record. The API expects one timestamp, the latest.
        // todo: Roman Zimine OM-52892 Allow the ability to specify which query is required.
        // TODO: Why do we only change the date/epoch when there are multiple stats?
        if (inputDTO.getStartDate() == null && inputDTO.getEndDate() == null && stats.size() > 1) {
            List<StatApiDTO> statistics = new ArrayList<>();
            for (StatSnapshotApiDTO stat: stats) {
                statistics.addAll(stat.getStatistics());
            }
            stats.clear();
            StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
            statSnapshotApiDTO.setDate(DateTimeUtil.getNow());
            statSnapshotApiDTO.setEpoch(Epoch.CURRENT);
            statSnapshotApiDTO.setStatistics(statistics);
            stats.add(statSnapshotApiDTO);
        }

        return StatsUtils.filterStats(stats, allowCoolingPower ? null : Collections.emptyList());
    }

    /**
     * This method checks if the input scope is a business account / billing family / group of
     * business accounts.
     *
     * @param scope The input scope.
     * @return whether the scope contains business account(s).
     */
    public static boolean scopeHasBusinessAccounts(@Nonnull final ApiId scope) {
        return scope.getScopeTypes().isPresent()
                && scope.getScopeTypes().get().contains(UIEntityType.BUSINESS_ACCOUNT);
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
     * Create the unique identifier for StatApiDTO by combining stat name, related entity and
     * filters' values.
     *
     * For example: a VM can buy multiple StorageAmounts (from multiple Storages), stat name is the
     * same, but the related entity is different. Also an entity can buy multiple same commodities
     * with different key.
     *
     * @param statApiDTO the StatApiDTO
     * @return identifier for the given StatApiDTO
     */
    private String createStatIdentifier(@Nonnull StatApiDTO statApiDTO) {
        StringBuilder sb = new StringBuilder();

        // stat name
        sb.append(statApiDTO.getName());

        // related entity
        final BaseApiDTO relatedEntity = statApiDTO.getRelatedEntity();
        if (relatedEntity != null && relatedEntity.getUuid() != null) {
            sb.append(relatedEntity.getUuid());
        }

        // related entity type
        if (statApiDTO.getRelatedEntityType() != null) {
            sb.append(statApiDTO.getRelatedEntityType());
        }

        // filters' values, sample filters: key, relation...
        List<StatFilterApiDTO> filters = statApiDTO.getFilters();
        if (filters != null) {
            sb.append(filters.stream()
                .sorted(Comparator.comparing(StatFilterApiDTO::getType))
                .map(StatFilterApiDTO::getValue)
                .filter(Objects::nonNull)
                .collect(Collectors.joining()));
        }

        return sb.toString();
    }

    /**
     * A helper class to disambiguate stat snapshots by the combination of time and epoch
     */
    private class StatTimeAndEpoch {

        private final long time;
        private final Epoch epoch;

        StatTimeAndEpoch(final long time, @Nullable final Epoch epoch) {
            this.time = time;
            this.epoch = epoch;
        }

        public long getTime() {
            return time;
        }

        @Nullable
        public Epoch getEpoch() {
            return epoch;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StatTimeAndEpoch)) {
                return false;
            }
            final StatTimeAndEpoch that = (StatTimeAndEpoch)o;
            return time == that.time &&
                epoch == that.epoch;
        }

        @Override
        public int hashCode() {
            return Objects.hash(time, epoch);
        }
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
