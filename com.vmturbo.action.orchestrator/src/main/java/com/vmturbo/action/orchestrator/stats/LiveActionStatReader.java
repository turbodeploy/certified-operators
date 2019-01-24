package com.vmturbo.action.orchestrator.stats;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore.MatchedActionGroups;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore.QueryResult;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats.ActionStatSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.GroupBy;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * Responsible for handling historical action stats queries for the "live" (or realtime)
 * topology context.
 */
public class LiveActionStatReader {

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

    public LiveActionStatReader(@Nonnull final ActionGroupStore actionGroupStore,
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
    public ActionStats readActionStats(@Nonnull final ActionDTO.HistoricalActionCountsQuery actionCountsQuery) {
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
            mgmtUnitSubgroupStore.query(actionCountsQuery.getMgmtUnitSubgroupFilter());
        // We don't support querying "all" management units, so if there is no result
        // there will be no stats. There may be no result - for example, if the
        // stats for the management unit haven't been saved yet.
        if (!mgmtUnitSubgroupResult.isPresent()) {
            logger.info("No mgmt unit subgroups matched the query. Returning empty stats.");
            return ActionStats.getDefaultInstance();
        }

        final Optional<Long> targetMgmtUnit = mgmtUnitSubgroupResult.get().mgmtUnit();
        final Set<Integer> targetMgmtUnitSubgroups = mgmtUnitSubgroupResult.get().mgmtUnitSubgroups();

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

        final Map<LocalDateTime, Map<ActionGroup, RolledUpActionGroupStat>> matchingStats =
            targetTableReader.query(actionCountsQuery.getTimeRange(),
                targetMgmtUnitSubgroups,
                applicableAgIds.get());

        final ActionStats.Builder retStatsBuilder = ActionStats.newBuilder();
        targetMgmtUnit.ifPresent(retStatsBuilder::setMgmtUnitId);

        matchingStats.forEach((time, statsByGroup) -> {
            final ActionStatSnapshot.Builder thisTimeSnapshot = ActionStatSnapshot.newBuilder()
                .setTime(time.toInstant(ZoneOffset.UTC).toEpochMilli());

            final CombinedStatsBuckets buckets =
                statsBucketsFactory.arrangeIntoBuckets(actionCountsQuery.getGroupBy(), statsByGroup);

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
    private List<String> validateQuery(@Nonnull final ActionDTO.HistoricalActionCountsQuery query) {
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
         * @param statsByGroup The {@link RolledUpActionGroupStat}s, arranged by their action group.
         * @return {@link CombinedStatsBuckets}, with one bucket for each distinct grouping
         *          criteria value (i.e. the stat values of all action groups that share the same
         *          grouping criteria will be combined into a single value). If the grouping
         *          criteria is "NONE", all stats will be combined into a single bucket.
         */
        @Nonnull
        CombinedStatsBuckets arrangeIntoBuckets(@Nonnull final GroupBy groupBy,
                                                @Nonnull final Map<ActionGroup, RolledUpActionGroupStat> statsByGroup);

        /**
         * The default implementation of {@link CombinedStatsBucketsFactory}.
         */
        class DefaultBucketsFactory implements CombinedStatsBucketsFactory {

            /**
             * {@inheritDoc}
             */
            @Override
            @Nonnull
            public CombinedStatsBuckets arrangeIntoBuckets(
                        @Nonnull final GroupBy groupBy,
                        @Nonnull final Map<ActionGroup, RolledUpActionGroupStat> statsByGroup) {
                final CombinedStatsBuckets buckets = new CombinedStatsBuckets();
                switch (groupBy) {
                    case NONE: {
                        // Everything gets aggregated into a single ActionStat.
                        buckets.createBucket(statsByGroup.keySet(), ActionDTO.ActionStat::newBuilder);
                        break;
                    }
                    case ACTION_STATE:
                        final Map<ActionState, List<ActionGroup>> groupsByState = statsByGroup.keySet().stream()
                            .collect(Collectors.groupingBy(ag -> ag.key().getActionState()));
                        groupsByState.forEach((state, groupsForState) -> {
                            buckets.createBucket(groupsForState,
                                () -> ActionDTO.ActionStat.newBuilder().setActionState(state));
                        });
                        break;
                    case ACTION_CATEGORY:
                        final Map<ActionCategory, List<ActionGroup>> groupsByCat = statsByGroup.keySet().stream()
                            .collect(Collectors.groupingBy(ag -> ag.key().getCategory()));
                        groupsByCat.forEach((category, groupsForCat) -> {
                            buckets.createBucket(groupsForCat,
                                () -> ActionDTO.ActionStat.newBuilder().setActionCategory(category));
                        });
                        break;
                    default:
                        logger.error("Unhandled split by: {}", groupBy);
                }

                statsByGroup.forEach(buckets::addStatsForGroup);
                return buckets;
            }
        }
    }

    /**
     * Represents a set of "buckets", where each bucket contains stats for action groups that
     * share a particular criteria.
     */
    static class CombinedStatsBuckets {
        private final Map<ActionGroup, CombinedStatsBucket> bucketForGroup = new HashMap<>();

        private final List<CombinedStatsBucket> buckets = new ArrayList<>();

        /**
         * Use {@link CombinedStatsBucketsFactory}.
         */
        private CombinedStatsBuckets() {}

        private void createBucket(@Nonnull final Collection<ActionGroup> bucketGroups,
                                 @Nonnull final Supplier<ActionDTO.ActionStat.Builder> statBuilderFactory) {
            final CombinedStatsBucket bucket = new CombinedStatsBucket(statBuilderFactory);
            bucketGroups.forEach(group -> bucketForGroup.put(group, bucket));
            buckets.add(bucket);
        }

        private void addStatsForGroup(@Nonnull final ActionGroup group,
                                     @Nonnull final RolledUpActionGroupStat statsForGroup) {
            final CombinedStatsBucket bucket = bucketForGroup.get(group);
            if (bucket != null) {
                bucket.add(statsForGroup);
            }
        }

        /**
         * @return the {@link ActionDTO.ActionStat} equivalent for each bucket.
         */
        @Nonnull
        Stream<ActionDTO.ActionStat> toActionStats() {
            return buckets.stream()
                .map(CombinedStatsBucket::toStat);
        }

        private static class CombinedStatsBucket {
            private final Supplier<ActionDTO.ActionStat.Builder> statBuilderFactory;

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

            CombinedStatsBucket(@Nonnull final Supplier<ActionDTO.ActionStat.Builder> statBuilderFactory) {
                this.statBuilderFactory = Objects.requireNonNull(statBuilderFactory);
            }

            public void add(@Nonnull final RolledUpActionGroupStat statsForGroup) {
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
                final ActionDTO.ActionStat.Builder statBuilder = statBuilderFactory.get();
                if (maxActionCount > 0) {
                    statBuilder.setActionCount(ActionDTO.ActionStat.Value.newBuilder()
                        .setAvg(avgActionCount)
                        .setMin(minActionCount)
                        .setMax(maxActionCount));
                }
                if (maxEntityCount > 0) {
                    statBuilder.setEntityCount(ActionDTO.ActionStat.Value.newBuilder()
                        .setAvg(avgEntityCount)
                        .setMin(minEntityCount)
                        .setMax(maxEntityCount));
                }
                if (maxSavings > 0) {
                    statBuilder.setSavings(ActionDTO.ActionStat.Value.newBuilder()
                        .setAvg(avgSavings)
                        .setMin(minSavings)
                        .setMax(maxSavings));
                }
                if (maxInvestment > 0) {
                    statBuilder.setInvestments(ActionDTO.ActionStat.Value.newBuilder()
                        .setAvg(avgInvestment)
                        .setMin(minInvestment)
                        .setMax(maxInvestment));
                }
                return statBuilder.build();
            }
        }
    }

    static class Metrics {
        static final DataMetricCounter QUERIES_COUNTER = DataMetricCounter.builder()
            .withName("ao_action_stat_query_count")
            .withHelp("Number of query requests received by the LiveActionStatReader")
            .build()
            .register();

        static final DataMetricCounter INVALID_QUERIES_COUNTER = DataMetricCounter.builder()
            .withName("ao_invalid_action_stat_query_count")
            .withHelp("Number of invalid query requests received by the LiveActionStatReader")
            .build()
            .register();
    }
}
