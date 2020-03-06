package com.vmturbo.action.orchestrator.stats;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore.MatchedActionGroups;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore.QueryResult;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.QueryResultsFromSnapshot;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats.ActionStatSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.GroupBy;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * Responsible for handling historical action stats queries for the "live" (or realtime)
 * topology context.
 */
public class HistoricalActionStatReader {

    private static final Logger logger = LogManager.getLogger();

    private final ActionGroupStore actionGroupStore;

    private final MgmtUnitSubgroupStore mgmtUnitSubgroupStore;

    private final TimeFrameCalculator timeFrameCalculator;

    /**
     * A map for {@link TimeFrame} to the {@link ActionStatTable.Reader} to use to
     * retrieve stats for the time frame. Each reader will query a different SQL table.
     */
    private final Map<TimeFrame, ActionStatTable.Reader> tablesForTimeFrame;

    private final CombinedStatsBucketsFactory statsBucketsFactory;

    public HistoricalActionStatReader(@Nonnull final ActionGroupStore actionGroupStore,
                                      @Nonnull final MgmtUnitSubgroupStore mgmtUnitSubgroupStore,
                                      @Nonnull final TimeFrameCalculator timeFrameCalculator,
                                      @Nonnull final Map<TimeFrame, ActionStatTable.Reader> tablesForTimeFrame,
                                      @Nonnull final CombinedStatsBucketsFactory statsBucketsFactory) {
        this.actionGroupStore = Objects.requireNonNull(actionGroupStore);
        this.mgmtUnitSubgroupStore = Objects.requireNonNull(mgmtUnitSubgroupStore);
        this.timeFrameCalculator = Objects.requireNonNull(timeFrameCalculator);
        this.tablesForTimeFrame = Objects.requireNonNull(tablesForTimeFrame);
        this.statsBucketsFactory = Objects.requireNonNull(statsBucketsFactory);
    }


    /**
     * Read action stats matching a query from the database.
     * This will only retrieve stats that have been saved and rolled up. (See:
     * {@link LiveActionsStatistician} for the write-side).
     *
     * @param actionCountsQuery The query specifying which stats to retrieve.
     * @return An {@link ActionStats} protobuf containing all the requested action stats.
     */
    @Nonnull
    public ActionStats readActionStats(@Nonnull final ActionDTO.HistoricalActionStatsQuery actionCountsQuery) {
        Metrics.QUERIES_COUNTER.increment();

        logger.trace("Reading action stats that match query: {}", actionCountsQuery);
        final List<String> validationErrors = validateQuery(actionCountsQuery);
        if (!validationErrors.isEmpty()) {
            logger.error("Query: {} has the following errors: {}. Returning empty stats.",
                actionCountsQuery, validationErrors);
            Metrics.INVALID_QUERIES_COUNTER.increment();
            return ActionStats.getDefaultInstance();
        }

        final Optional<QueryResult> mgmtUnitSubgroupResult =
            mgmtUnitSubgroupStore.query(actionCountsQuery.getMgmtUnitSubgroupFilter(), actionCountsQuery.getGroupBy());
        // We don't support querying "all" management units, so if there is no result
        // there will be no stats. There may be no result - for example, if the
        // stats for the management unit haven't been saved yet.
        if (!mgmtUnitSubgroupResult.isPresent()) {
            logger.info("No mgmt unit subgroups matched the query. Returning empty stats.");
            return ActionStats.getDefaultInstance();
        }

        final Optional<Long> targetMgmtUnit = mgmtUnitSubgroupResult.get().mgmtUnit();
        final Map<Integer, MgmtUnitSubgroup> targetMgmtUnitSubgroups = mgmtUnitSubgroupResult.get().mgmtUnitSubgroups();

        // Find action groups after finding the mgmt unit, so that we don't issue a query
        // if there is no target mgmt unit.
        final Optional<MatchedActionGroups> applicableAgIds =
            actionGroupStore.query(actionCountsQuery.getActionGroupFilter());
        if (!applicableAgIds.isPresent()) {
            logger.info("No action groups matched the query. Returning empty stats.");
            return ActionStats.getDefaultInstance();
        }

        // We use the start time to determine the time frame to query.
        // This means that if the end time has not been rolled up yet we may miss the last time unit.
        final TimeFrame timeFrame =
            timeFrameCalculator.millis2TimeFrame(actionCountsQuery.getTimeRange().getStartTime());
        // Select the right reader to use based on the time frame.
        final ActionStatTable.Reader targetTableReader = tablesForTimeFrame.get(timeFrame);

        final List<QueryResultsFromSnapshot> queryResultsFromSnapshots =
            targetTableReader.query(actionCountsQuery.getTimeRange(),
                targetMgmtUnitSubgroups.keySet(),
                applicableAgIds.get());

        final ActionStats.Builder retStatsBuilder = ActionStats.newBuilder();
        targetMgmtUnit.ifPresent(retStatsBuilder::setMgmtUnitId);

        queryResultsFromSnapshots.forEach(queryResult -> {
            final ActionStatSnapshot.Builder thisTimeSnapshot = ActionStatSnapshot.newBuilder()
                .setTime(queryResult.time().toInstant(ZoneOffset.UTC).toEpochMilli());
            final CombinedStatsBuckets buckets =
                statsBucketsFactory.arrangeIntoBuckets(actionCountsQuery.getGroupBy(),
                    queryResult.numActionSnapshots(),
                    queryResult.statsByGroupAndMu(),
                    targetMgmtUnitSubgroups);

            buckets.toActionStats().forEach(thisTimeSnapshot::addStats);

            retStatsBuilder.addStatSnapshots(thisTimeSnapshot);
        });

        return retStatsBuilder.build();
    }

    /**
     *
     * @param query The query to validate.
     * @return A list of human-readable errors with the query.
     */
    private List<String> validateQuery(@Nonnull final ActionDTO.HistoricalActionStatsQuery query) {
        final List<String> errors = new ArrayList<>();
        if (query.getTimeRange().getStartTime() == 0 ) {
            errors.add("Query has no start time set.");
        }
        if (query.getTimeRange().getEndTime() == 0) {
            errors.add("Query has no end time set.");
        }

        if (!query.hasMgmtUnitSubgroupFilter()) {
            errors.add("Query has no mgmt unit subgroup filter.");
        }

        return errors;
    }

    /**
     * A factory for {@link CombinedStatsBuckets}, used for dependency injection in unit tests.
     */
    @FunctionalInterface
    interface CombinedStatsBucketsFactory {

        /**
         * Arrange {@link RolledUpActionGroupStat}s into buckets based on a grouping
         * criteria.
         *
         * @param groupBy The grouping criteria that defines how to divide action group stats
         *                into buckets. For example, if the criteria is "CATEGORY" then all
         *                action groups that share the same category will be combined into a single
         *                bucket.
         * @param numSnapshots The number of snapshots that the stats were aggregated across.
         * @param statsByGroupAndMu The {@link RolledUpActionGroupStat}s, arranged by their action group
         *                          and management unit subgroup.
         * @param muById The {@link MgmtUnitSubgroup}s, arranged by id.
         * @return {@link CombinedStatsBuckets}, with one bucket for each distinct grouping
         *          criteria value (i.e. the stat values of all action groups that share the same
         *          grouping criteria will be combined into a single value). If the grouping
         *          criteria is "NONE", all stats will be combined into a single bucket.
         */
        @Nonnull
        CombinedStatsBuckets arrangeIntoBuckets(@Nonnull GroupBy groupBy,
                int numSnapshots,
                @Nonnull Map<ActionGroup, Map<Integer, RolledUpActionGroupStat>> statsByGroupAndMu,
                @Nonnull Map<Integer, MgmtUnitSubgroup> muById);

        /**
         * The default implementation of {@link CombinedStatsBucketsFactory}.
         */
        class DefaultBucketsFactory implements CombinedStatsBucketsFactory {

            @Override
            @Nonnull
            public CombinedStatsBuckets arrangeIntoBuckets(@Nonnull final GroupBy groupBy,
                    final int numSnapshots,
                    @Nonnull final Map<ActionGroup, Map<Integer, RolledUpActionGroupStat>> statsByGroupAndMu,
                    @Nonnull final Map<Integer, MgmtUnitSubgroup> muById) {
                final CombinedStatsBuckets buckets = new CombinedStatsBuckets();
                switch (groupBy) {
                    case NONE: {
                        // Everything goes into a single bucket.
                        final GroupByBucketKey bucketKey = ImmutableGroupByBucketKey.builder().build();
                        buckets.addStatsForGroup(bucketKey, numSnapshots, statsByGroupAndMu.values().stream()
                            .flatMap(statsByMu -> statsByMu.values().stream()));
                        break;
                    }
                    case ACTION_STATE:
                        final Map<ActionState, List<Collection<RolledUpActionGroupStat>>> statsByState =
                            statsByGroupAndMu.entrySet().stream()
                                .collect(Collectors.groupingBy(ag -> ag.getKey().key().getActionState(),
                                    Collectors.mapping(e -> e.getValue().values(), Collectors.toList())));
                        statsByState.forEach((distinctState, statsForAllMu) -> {
                            final GroupByBucketKey bucketKey = ImmutableGroupByBucketKey.builder()
                                .state(distinctState)
                                .build();
                            buckets.addStatsForGroup(bucketKey, numSnapshots, statsForAllMu.stream()
                                .flatMap(Collection::stream));
                        });
                        break;
                    case ACTION_CATEGORY:
                        final Map<ActionCategory, List<Collection<RolledUpActionGroupStat>>> statsByCat =
                            statsByGroupAndMu.entrySet().stream()
                                .collect(Collectors.groupingBy(ag -> ag.getKey().key().getCategory(),
                                    Collectors.mapping(e -> e.getValue().values(), Collectors.toList())));
                        statsByCat.forEach((distinctCategory, statsForAllMu) -> {
                            final GroupByBucketKey bucketKey = ImmutableGroupByBucketKey.builder()
                                .category(distinctCategory)
                                .build();
                            buckets.addStatsForGroup(bucketKey, numSnapshots, statsForAllMu.stream()
                                .flatMap(Collection::stream));
                        });
                        break;
                    case BUSINESS_ACCOUNT_ID:
                        final Map<Long, List<RolledUpActionGroupStat>> statsByBusinessAccount = new HashMap<>();
                        statsByGroupAndMu.forEach((group, statsByMu) -> {
                            statsByMu.forEach((muId, stats) -> {
                                final MgmtUnitSubgroup mu = muById.get(muId);
                                if (mu != null && mu.key().mgmtUnitType() == ManagementUnitType.BUSINESS_ACCOUNT) {
                                    statsByBusinessAccount.computeIfAbsent(mu.key().mgmtUnitId(), k -> new ArrayList<>())
                                        .add(stats);
                                } else if (mu != null) {
                                    // This shouldn't happen, unless we found the wrong mgmt unit types
                                    // upstream.
                                    logger.warn("Unexpected management unit subgroup {} when grouping by business account." +
                                        " Expected all relevant management subgroups to have type {}.",
                                        mu, ManagementUnitType.BUSINESS_ACCOUNT);
                                } else {
                                    // This shouldn't happen, because the management units in the
                                    // full query response should be a subset of the management units
                                    // in the initial "mgmt unit subgroup" search.
                                    logger.warn("Management unit {} missing from index.", muId);
                                }
                            });
                        });
                        statsByBusinessAccount.forEach((distinctAccountId, stats) -> {
                            final GroupByBucketKey bucketKey = ImmutableGroupByBucketKey.builder()
                                .businessAccountId(distinctAccountId)
                                .build();
                            buckets.addStatsForGroup(bucketKey, numSnapshots, stats.stream());
                        });
                        break;
                    default:
                        logger.error("Unhandled split by: {}", groupBy);
                }

                return buckets;
            }
        }
    }

    /**
     * Represents a set of "buckets", where each bucket contains stats for action groups that
     * share a particular criteria.
     */
    static class CombinedStatsBuckets {
        private final Map<GroupByBucketKey, CombinedStatsBucket> bucketsByKey = new HashMap<>();

        /**
         * Use {@link CombinedStatsBucketsFactory}.
         */
        private CombinedStatsBuckets() {}

        private void addStatsForGroup(@Nonnull final GroupByBucketKey bucketKey,
                                     final int numSnapshots,
                                     @Nonnull final Stream<RolledUpActionGroupStat> statsForGroup) {
            final CombinedStatsBucket bucket = bucketsByKey.computeIfAbsent(bucketKey,
                k -> new CombinedStatsBucket(numSnapshots, bucketKey.toStatGroup()));
            statsForGroup.forEach(bucket::add);
        }

        /**
         * @return the {@link ActionDTO.ActionStat} equivalent for each bucket.
         */
        @Nonnull
        Stream<ActionDTO.ActionStat> toActionStats() {
            return bucketsByKey.values().stream()
                .map(CombinedStatsBucket::toStat);
        }

        private static class CombinedStatsBucket {
            private final ActionDTO.ActionStat.StatGroup statGroup;

            private int numSnapshots;

            private int       totalActionCount = 0;

            private int       avgActionCount = 0;
            private int       minActionCount = 0;
            private int       maxActionCount = 0;

            private int       avgEntityCount = 0;
            private int       minEntityCount = 0;
            private int       maxEntityCount = 0;

            private double    avgSavings = 0;
            private double    minSavings = 0;
            private double    maxSavings = 0;

            private double    avgInvestment = 0;
            private double    minInvestment = 0;
            private double    maxInvestment = 0;

            CombinedStatsBucket(final int numSnapshots,
                                @Nonnull final ActionDTO.ActionStat.StatGroup statGroup) {
                this.numSnapshots = numSnapshots;
                this.statGroup = Objects.requireNonNull(statGroup);
            }

            public void add(@Nonnull final RolledUpActionGroupStat statsForGroup) {

                totalActionCount += statsForGroup.newActionCount() +
                    statsForGroup.priorActionCount();

                avgActionCount += statsForGroup.avgActionCount();
                minActionCount += statsForGroup.minActionCount();
                maxActionCount += statsForGroup.maxActionCount();

                avgEntityCount += statsForGroup.avgEntityCount();
                minEntityCount += statsForGroup.minEntityCount();
                maxEntityCount += statsForGroup.maxEntityCount();

                avgSavings += statsForGroup.avgSavings();
                minSavings += statsForGroup.minSavings();
                maxSavings += statsForGroup.maxSavings();

                avgInvestment += statsForGroup.avgInvestment();
                minInvestment += statsForGroup.minInvestment();
                maxInvestment += statsForGroup.maxInvestment();
            }

            @Nonnull
            ActionDTO.ActionStat toStat() {
                final ActionDTO.ActionStat.Builder statBuilder = ActionDTO.ActionStat.newBuilder()
                    .setStatGroup(statGroup);
                if (maxActionCount > 0) {
                    statBuilder.setActionCount(ActionDTO.ActionStat.Value.newBuilder()
                        .setAvg(avgActionCount)
                        .setMin(minActionCount)
                        .setMax(maxActionCount)
                        .setTotal(totalActionCount));
                }
                if (maxEntityCount > 0) {
                    statBuilder.setEntityCount(ActionDTO.ActionStat.Value.newBuilder()
                        .setAvg(avgEntityCount)
                        .setMin(minEntityCount)
                        .setMax(maxEntityCount)
                        .setTotal(avgEntityCount * numSnapshots));
                }
                if (maxSavings > 0) {
                    statBuilder.setSavings(ActionDTO.ActionStat.Value.newBuilder()
                        .setAvg(avgSavings)
                        .setMin(minSavings)
                        .setMax(maxSavings)
                        .setTotal(avgSavings * numSnapshots));
                }
                if (maxInvestment > 0) {
                    statBuilder.setInvestments(ActionDTO.ActionStat.Value.newBuilder()
                        .setAvg(avgInvestment)
                        .setMin(minInvestment)
                        .setMax(maxInvestment)
                        .setTotal(avgInvestment * numSnapshots));
                }
                return statBuilder.build();
            }
        }
    }

    /**
     * Identifies the "key" of a bucket used to group action stats.
     * The "key" is the unique combination of bucket identifiers.
     */
    @Value.Immutable
    public interface GroupByBucketKey {
        /**
         * The {@link ActionState} for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the state.
         */
        Optional<ActionState> state();

        /**
         * The {@link ActionCategory} for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the category.
         */
        Optional<ActionCategory> category();

        /**
         * The business account associated with the actions in this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the business account.
         */
        Optional<Long> businessAccountId();

        /**
         * Convert to a protobuf stat group that can be returned to gRPC users.
         *
         * @return The {@link ActionDTO.ActionStat.StatGroup}.
         */
        default ActionDTO.ActionStat.StatGroup toStatGroup() {
            ActionDTO.ActionStat.StatGroup.Builder bldr = ActionDTO.ActionStat.StatGroup.newBuilder();
            state().ifPresent(bldr::setActionState);
            category().ifPresent(bldr::setActionCategory);
            businessAccountId().ifPresent(bldr::setBusinessAccountId);
            return bldr.build();
        }
    }

    static class Metrics {
        static final DataMetricCounter QUERIES_COUNTER = DataMetricCounter.builder()
            .withName("ao_historical_action_stat_query_count")
            .withHelp("Number of query requests received by the HistoricalActionStatReader")
            .build()
            .register();

        static final DataMetricCounter INVALID_QUERIES_COUNTER = DataMetricCounter.builder()
            .withName("ao_invalid_historical_action_stat_query_count")
            .withHelp("Number of invalid query requests received by the HistoricalActionStatReader")
            .build()
            .register();
    }
}
