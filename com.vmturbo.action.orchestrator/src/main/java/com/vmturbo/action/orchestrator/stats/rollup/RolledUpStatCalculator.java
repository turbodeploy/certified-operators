package com.vmturbo.action.orchestrator.stats.rollup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.jooq.Record;

import com.google.common.base.Preconditions;

import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.BaseActionStatTableReader.StatWithSnapshotCnt;

/**
 * A calculator that can aggregate database records from the various action stat tables into
 * a {@link RolledUpActionGroupStat}.
 */
public class RolledUpStatCalculator {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    public Optional<RolledUpActionGroupStat> rollupLatestRecords(final int numSnapshotsInRange,
             @Nonnull final List<StatWithSnapshotCnt<ActionStatsLatestRecord>> actionStats) {
        // This looks very similar to rollupHourRecords and rollupDayRecords, but we're working
        // with the jOOQ-generated ActionStatsLatestRecord instead.
        logger.trace("Calculating rolled up stats for {} latest records," +
                " from {} snapshots in range.",
            actionStats.size(), numSnapshotsInRange);

        if (actionStats.isEmpty()) {
            return Optional.empty();
        }

        if (!validateSingleId(actionStats,
            record -> record.record().getActionGroupId(),
            record -> record.record().getMgmtUnitSubgroupId())) {
            return Optional.empty();
        }

        int minActionCount = Integer.MAX_VALUE;
        int minEntityCount = Integer.MAX_VALUE;
        double minInvestment = Double.MAX_VALUE;
        double minSavings = Double.MAX_VALUE;

        int maxActionCount = Integer.MIN_VALUE;
        int maxEntityCount = Integer.MIN_VALUE;
        double maxInvestment = Double.MIN_VALUE;
        double maxSavings = Double.MIN_VALUE;

        // We could use AverageSummary for latest records - like we do for hour and day - but
        // that adds extra and unnecessary object allocation. Since we
        // don't need to weigh the average of each record, we calculate the average
        // directly (avg = total / numSnapshotsInRange for each thing we're tracking).
        int totalEntities = 0;
        double totalInvestment = 0;
        double totalSavings = 0;

        // total actions = "prior actions for first stat" + sum("new actions over time range")
        // calculate the "prior actions" indirectly from "total actions" - "new actions" for
        // first stat of this time range
        final StatWithSnapshotCnt<ActionStatsLatestRecord> firstStatOfTimeRange =
            actionStats.iterator().next();
        // calculate the "old actions" indirectly from "total actions" - "new actions"
        int initialTotalActions = firstStatOfTimeRange.record().getTotalActionCount() - firstStatOfTimeRange.record().getNewActionCount();
        int totalActions = initialTotalActions;
        int newActions = 0;

        for (final StatWithSnapshotCnt<ActionStatsLatestRecord> recordAndCount : actionStats) {
            // Each "latest" record can only represent one action snapshot.
            Preconditions.checkArgument(recordAndCount.numActionSnapshots() == 1);
            final ActionStatsLatestRecord actionStat = recordAndCount.record();
            minActionCount = Math.min(minActionCount, actionStat.getTotalActionCount());
            minEntityCount = Math.min(minEntityCount, actionStat.getTotalEntityCount());
            minInvestment = Math.min(minInvestment, actionStat.getTotalInvestment().doubleValue());
            minSavings = Math.min(minSavings, actionStat.getTotalSavings().doubleValue());

            maxActionCount = Math.max(maxActionCount, actionStat.getTotalActionCount());
            maxEntityCount = Math.max(maxEntityCount, actionStat.getTotalEntityCount());
            maxInvestment = Math.max(maxInvestment, actionStat.getTotalInvestment().doubleValue());
            maxSavings = Math.max(maxSavings, actionStat.getTotalSavings().doubleValue());

            totalActions += actionStat.getNewActionCount();
            newActions += actionStat.getNewActionCount();
            totalEntities += actionStat.getTotalEntityCount();
            totalInvestment += actionStat.getTotalInvestment().doubleValue();
            totalSavings += actionStat.getTotalSavings().doubleValue();
        }

        return Optional.of(ImmutableRolledUpActionGroupStat.builder()
            .minActionCount(minActionCount)
            .minEntityCount(minEntityCount)
            .minInvestment(minInvestment)
            .minSavings(minSavings)
            .maxActionCount(maxActionCount)
            .maxEntityCount(maxEntityCount)
            .maxInvestment(maxInvestment)
            .maxSavings(maxSavings)
            .priorActionCount(initialTotalActions)
            .newActionCount(newActions)
            .avgActionCount((double)totalActions / numSnapshotsInRange)
            .avgEntityCount((double)totalEntities / numSnapshotsInRange)
            .avgInvestment(totalInvestment / numSnapshotsInRange)
            .avgSavings(totalSavings / numSnapshotsInRange)
            .build());
    }

    @Nonnull
    public Optional<RolledUpActionGroupStat> rollupHourRecords(final int numActionPlanSnapshotsInRange,
                   @Nonnull final List<StatWithSnapshotCnt<ActionStatsByHourRecord>> actionStats) {
        // This looks very similar to rollupDayRecords and rollupLatestRecords, but we're working
        // with the jOOQ-generated ActionStatsByHourRecord instead.
        logger.trace("Calculating rolled up stats for {} hourly records.", actionStats.size());
        if (actionStats.isEmpty()) {
            return Optional.empty();
        }

        if (!validateSingleId(actionStats,
                record -> record.record().getActionGroupId(),
                record -> record.record().getMgmtUnitSubgroupId())) {
            return Optional.empty();
        }

        int minActionCount = Integer.MAX_VALUE;
        int minEntityCount = Integer.MAX_VALUE;
        double minInvestment = Double.MAX_VALUE;
        double minSavings = Double.MAX_VALUE;

        int maxActionCount = Integer.MIN_VALUE;
        int maxEntityCount = Integer.MIN_VALUE;
        double maxInvestment = Double.MIN_VALUE;
        double maxSavings = Double.MIN_VALUE;

        final List<AverageSummary> avgSummaries = new ArrayList<>(actionStats.size());

        // total actions = "prior actions for first stat" + sum("new actions over time range")
        final StatWithSnapshotCnt<ActionStatsByHourRecord> firstStatOfTimeRange =
            actionStats.iterator().next();
        final int priorActionCount = firstStatOfTimeRange.record().getPriorActionCount();
        int newActions = 0;

        for (StatWithSnapshotCnt<ActionStatsByHourRecord> recordAndCount : actionStats) {
            ActionStatsByHourRecord actionStat = recordAndCount.record();
            newActions += actionStat.getNewActionCount();
            minActionCount = Math.min(minActionCount, actionStat.getMinActionCount());
            minEntityCount = Math.min(minEntityCount, actionStat.getMinEntityCount());
            minInvestment = Math.min(minInvestment, actionStat.getMinInvestment().doubleValue());
            minSavings = Math.min(minSavings, actionStat.getMinSavings().doubleValue());

            maxActionCount = Math.max(maxActionCount, actionStat.getMaxActionCount());
            maxEntityCount = Math.max(maxEntityCount, actionStat.getMaxEntityCount());
            maxInvestment = Math.max(maxInvestment, actionStat.getMaxInvestment().doubleValue());
            maxSavings = Math.max(maxSavings, actionStat.getMaxSavings().doubleValue());

            avgSummaries.add(ImmutableAverageSummary.builder()
                .numSnapshots(recordAndCount.numActionSnapshots())
                .avgActionCount(actionStat.getAvgActionCount().doubleValue())
                .avgEntityCount(actionStat.getAvgEntityCount().doubleValue())
                .avgInvestment(actionStat.getAvgInvestment().doubleValue())
                .avgSavings(actionStat.getAvgSavings().doubleValue())
                .build());
        }

        final AverageSummary totalAvgSummary = AverageSummary.combine(numActionPlanSnapshotsInRange, avgSummaries);

        final RolledUpActionGroupStat rolledUpStat = ImmutableRolledUpActionGroupStat.builder()
            .minActionCount(minActionCount)
            .minEntityCount(minEntityCount)
            .minInvestment(minInvestment)
            .minSavings(minSavings)
            .maxActionCount(maxActionCount)
            .maxEntityCount(maxEntityCount)
            .maxInvestment(maxInvestment)
            .maxSavings(maxSavings)
            .priorActionCount(priorActionCount)
            .newActionCount(newActions)
            .avgActionCount(totalAvgSummary.avgActionCount())
            .avgEntityCount(totalAvgSummary.avgEntityCount())
            .avgInvestment(totalAvgSummary.avgInvestment())
            .avgSavings(totalAvgSummary.avgSavings())
            .build();
        return Optional.of(rolledUpStat);
    }

    @Nonnull
    public Optional<RolledUpActionGroupStat> rollupDayRecords(final int numActionPlanSnapshotsInRange,
              @Nonnull final List<StatWithSnapshotCnt<ActionStatsByDayRecord>> actionStats) {
        // This looks very similar to rollupHourRecords and rollupLatestRecords, but we're working
        // with the jOOQ-generated ActionStatsByDayRecord instead.
        logger.trace("Calculating rolled up stats for {} day records.", actionStats.size());
        if (actionStats.isEmpty()) {
            return Optional.empty();
        }

        if (!validateSingleId(actionStats,
            record -> record.record().getActionGroupId(),
            record -> record.record().getMgmtUnitSubgroupId())) {
            return Optional.empty();
        }

        int minActionCount = Integer.MAX_VALUE;
        int minEntityCount = Integer.MAX_VALUE;
        double minInvestment = Double.MAX_VALUE;
        double minSavings = Double.MAX_VALUE;

        int maxActionCount = Integer.MIN_VALUE;
        int maxEntityCount = Integer.MIN_VALUE;
        double maxInvestment = Double.MIN_VALUE;
        double maxSavings = Double.MIN_VALUE;

        // total actions = "prior actions for first stat" + sum("new actions over time range")
        final StatWithSnapshotCnt<ActionStatsByDayRecord> firstStatOfTimeRange =
            actionStats.iterator().next();
        final int priorActionCount = firstStatOfTimeRange.record().getPriorActionCount();
        int newActions = 0;
        final List<AverageSummary> avgSummaries = new ArrayList<>(actionStats.size());
        for (StatWithSnapshotCnt<ActionStatsByDayRecord> recordAndCount : actionStats) {
            final ActionStatsByDayRecord actionStat = recordAndCount.record();
            newActions += actionStat.getNewActionCount();
            minActionCount = Math.min(minActionCount, actionStat.getMinActionCount());
            minEntityCount = Math.min(minEntityCount, actionStat.getMinEntityCount());
            minInvestment = Math.min(minInvestment, actionStat.getMinInvestment().doubleValue());
            minSavings = Math.min(minSavings, actionStat.getMinSavings().doubleValue());

            maxActionCount = Math.max(maxActionCount, actionStat.getMaxActionCount());
            maxEntityCount = Math.max(maxEntityCount, actionStat.getMaxEntityCount());
            maxInvestment = Math.max(maxInvestment, actionStat.getMaxInvestment().doubleValue());
            maxSavings = Math.max(maxSavings, actionStat.getMaxSavings().doubleValue());

            avgSummaries.add(ImmutableAverageSummary.builder()
                .numSnapshots(recordAndCount.numActionSnapshots())
                .avgActionCount(actionStat.getAvgActionCount().doubleValue())
                .avgEntityCount(actionStat.getAvgEntityCount().doubleValue())
                .avgInvestment(actionStat.getAvgInvestment().doubleValue())
                .avgSavings(actionStat.getAvgSavings().doubleValue())
                .build());
        }

        final AverageSummary totalAvgSummary = AverageSummary.combine(numActionPlanSnapshotsInRange, avgSummaries);

        return Optional.of(ImmutableRolledUpActionGroupStat.builder()
            .minActionCount(minActionCount)
            .minEntityCount(minEntityCount)
            .minInvestment(minInvestment)
            .minSavings(minSavings)
            .maxActionCount(maxActionCount)
            .maxEntityCount(maxEntityCount)
            .maxInvestment(maxInvestment)
            .maxSavings(maxSavings)
            .priorActionCount(priorActionCount)
            .newActionCount(newActions)
            .avgActionCount(totalAvgSummary.avgActionCount())
            .avgEntityCount(totalAvgSummary.avgEntityCount())
            .avgInvestment(totalAvgSummary.avgInvestment())
            .avgSavings(totalAvgSummary.avgSavings())
            .build());
    }

    /**
     * The summary of averages in an {@link RolledUpActionGroupStat}.
     * Used to isolate/encapsulate weighted average calculation.
     */
    @Value.Immutable
    interface AverageSummary {
        double avgActionCount();
        double avgEntityCount();
        double avgInvestment();
        double avgSavings();

        int numSnapshots();

        /**
         * Combine a list of {@link AverageSummary} into a single {@link AverageSummary}. Each
         * {@link AverageSummary}'s number of snapshots affects how much it contributes to the
         * total average.
         *
         * @param summaries The list of {@link AverageSummary}. Should not be empty.
         * @return A single {@link AverageSummary} containing the averages across all summaries
         *         in the input.
         */
        @Nonnull
        static AverageSummary combine(final int totalActionPlanSnapshots,
                                      @Nonnull final List<AverageSummary> summaries) {
            final int totalSnapshots = summaries.stream()
                .mapToInt(AverageSummary::numSnapshots)
                .sum();
            Preconditions.checkArgument(totalActionPlanSnapshots >= totalSnapshots);

            double totalAvgActionCount = 0;
            double totalAvgEntityCount = 0;
            double totalAvgInvestment = 0;
            double totalAvgSavings = 0;
            for (AverageSummary summary : summaries) {
                final double fraction = (double)summary.numSnapshots() / totalActionPlanSnapshots;
                totalAvgActionCount += summary.avgActionCount() * fraction;
                totalAvgEntityCount += summary.avgEntityCount() * fraction;
                totalAvgInvestment += summary.avgInvestment() * fraction;
                totalAvgSavings += summary.avgSavings() * fraction;
            }

            return ImmutableAverageSummary.builder()
                .numSnapshots(totalActionPlanSnapshots)
                .avgActionCount(totalAvgActionCount)
                .avgEntityCount(totalAvgEntityCount)
                .avgInvestment(totalAvgInvestment)
                .avgSavings(totalAvgSavings)
                .build();
        }
    }

    /**
     * Check that a stream of action stats records contains only records for a single (action group,
     * mgmt unit subgroup) tuple. i.e. all records in the stream should have the same action group
     * and mgmt unit subgroup.
     *
     * @param records The list of action stats records.
     * @param actionGroupExtractor Function to extract the action group ID from a record.
     * @param mgmtUnitSubgroupExtractor Function to extract the mgmt unit subgroup ID from a record.
     * @return True if all records in the stream refer to the same {@link ActionGroup} and
     *         {@link MgmtUnitSubgroup}.
     */
    private <STAT_RECORD extends Record> boolean validateSingleId(@Nonnull final List<StatWithSnapshotCnt<STAT_RECORD>> records,
                                @Nonnull final Function<StatWithSnapshotCnt<STAT_RECORD>, Integer> actionGroupExtractor,
                                @Nonnull final Function<StatWithSnapshotCnt<STAT_RECORD>, Integer> mgmtUnitSubgroupExtractor) {
        // We try to avoid allocating unnecessary sets unless we know the list of records
        // contains references to different action groups/mgmt unit subgroups.
        Set<Integer> actionGroupIdSet = Collections.emptySet();
        Set<Integer> mgmtSubgroupIdSet = Collections.emptySet();
        Integer firstActionGroupId = null;
        Integer firstMgmtSubgroupId = null;
        for (StatWithSnapshotCnt<STAT_RECORD> record : records) {
            final Integer curActionGroupId = actionGroupExtractor.apply(record);
            final Integer curMgmtSubgroupId = mgmtUnitSubgroupExtractor.apply(record);
            // This should only happen once - on the first record.
            if (firstActionGroupId == null) {
                firstActionGroupId = curActionGroupId;
                firstMgmtSubgroupId = curMgmtSubgroupId;
            } else {
                // Check if the current action group ID is different from the first.
                // In the regular (non-error) case, this will never be true.
                if (!firstActionGroupId.equals(curActionGroupId)) {
                    // If this is the first "failing" ID, we need to allocate a set to track
                    // all the different referenced action group IDs.
                    if (actionGroupIdSet.isEmpty()) {
                        actionGroupIdSet = new HashSet<>();
                        actionGroupIdSet.add(firstActionGroupId);
                    }
                    actionGroupIdSet.add(curActionGroupId);
                }

                // Do the same for mgmt unit subgroups.
                if (!firstMgmtSubgroupId.equals(curMgmtSubgroupId)) {
                    if (mgmtSubgroupIdSet.isEmpty()) {
                        mgmtSubgroupIdSet = new HashSet<>();
                        mgmtSubgroupIdSet.add(firstMgmtSubgroupId);
                    }
                    mgmtSubgroupIdSet.add(curMgmtSubgroupId);
                }
            }
        }

        if (actionGroupIdSet.size() > 1) {
            logger.error("All records should have exactly one action group ID. " +
                "Found {} IDS: {}.", actionGroupIdSet.size(), actionGroupIdSet);
        }

        if (mgmtSubgroupIdSet.size() > 1) {
            logger.error("All records should have exactly one mgmt unit subgroup ID. " +
                "Found {} IDS: {}.", actionGroupIdSet.size(), actionGroupIdSet);
        }

        return actionGroupIdSet.size() <= 1 && mgmtSubgroupIdSet.size() <= 1;
    }
}
