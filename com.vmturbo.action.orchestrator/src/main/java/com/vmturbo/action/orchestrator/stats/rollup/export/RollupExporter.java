package com.vmturbo.action.orchestrator.stats.rollup.export;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.api.export.ActionRollupExport;
import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.ActionRollupNotification;
import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.HourlyActionStat;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionStats;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Exports rolled up action stats like historical pending action count.
 */
public class RollupExporter implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger();

    private final ExecutorService executorService;

    private final LinkedBlockingQueue<Pair<Integer, RolledUpActionStats>> rolledUpStats = new LinkedBlockingQueue<>();

    private final boolean enableRollupExport;

    /**
     * Create a stat rollup exporter.
     *
     * @param rollupNotificationSender the sender to broadcast the stat rollups
     * @param mgmtUnitSubgroupStore a cache of management unit subgroups
     * @param actionGroupStore data store for action stats
     * @param clock to track the rollup
     * @param enableRollupExport the export will only take place if this is true
     */
    public RollupExporter(final IMessageSender<ActionRollupNotification> rollupNotificationSender,
            @Nonnull final MgmtUnitSubgroupStore mgmtUnitSubgroupStore,
            @Nonnull final ActionGroupStore actionGroupStore,
            @Nonnull final Clock clock,
            @Nonnull final boolean enableRollupExport) {
        this(rollupNotificationSender, mgmtUnitSubgroupStore, actionGroupStore, clock, enableRollupExport,
                Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setNameFormat("rollup-exporter-%d").build()));
    }

    /**
     * Create a stat rollup exporter.
     *
     * @param rollupNotificationSender the sender to broadcast the stat rollups
     * @param mgmtUnitSubgroupStore a cache of management unit subgroups
     * @param actionGroupStore data store for action stats
     * @param clock to track the rollup
     * @param enableRollupExport the export will only take place if this is true
     * @param executorService to schedule the rollup threads
     */
    public RollupExporter(final IMessageSender<ActionRollupNotification> rollupNotificationSender,
            @Nonnull final MgmtUnitSubgroupStore mgmtUnitSubgroupStore,
            @Nonnull final ActionGroupStore actionGroupStore,
            @Nonnull final Clock clock,
            @Nonnull final boolean enableRollupExport,
            @Nonnull final ExecutorService executorService) {
        this.executorService = executorService;
        this.enableRollupExport = enableRollupExport;
        if (enableRollupExport) {
            this.executorService.submit(
                    new RollupExportWorker(rollupNotificationSender, mgmtUnitSubgroupStore,
                            actionGroupStore, clock, rolledUpStats));
        }
    }

    /**
     * Export a single stat rollup.
     *
     * @param mgmtUnitSubgroupId the management unit sub group
     * @param rolledUpActionStats the rolled up stats to export
     */
    public void exportRollup(int mgmtUnitSubgroupId, RolledUpActionStats rolledUpActionStats) {
        if (enableRollupExport) {
            logger.debug("Received exported rollup for msu {} and {} action groups. Start time {}",
                    mgmtUnitSubgroupId, rolledUpActionStats.numActionSnapshots(),
                    rolledUpActionStats.startTime());
            rolledUpStats.add(Pair.of(mgmtUnitSubgroupId, rolledUpActionStats));
        }
    }

    @Override
    public void close() throws Exception {
        executorService.shutdownNow();
    }

    /**
     * The export worker reads batches of rolled up stats from the queue and exports them to Kafka.
     * We use a worker on a separate thread because individual mgmt unit subgroups are rolled up
     * on different threads, and looking up the associated mgmt unit subgroups and action groups
     * independently on all those threads is very wasteful.
     */
    @VisibleForTesting
    static class RollupExportWorker implements Runnable {
        private final IMessageSender<ActionRollupNotification> rollupNotificationSender;
        private final long pollTimeoutMins = 10;
        private final MgmtUnitSubgroupStore mgmtUnitSubgroupStore;
        private final ActionGroupStore actionGroupStore;
        private final Clock clock;
        private final LinkedBlockingQueue<Pair<Integer, RolledUpActionStats>> rolledUpStats;

        RollupExportWorker(final IMessageSender<ActionRollupNotification> rollupNotificationSender,
                final MgmtUnitSubgroupStore mgmtUnitSubgroupStore,
                final ActionGroupStore actionGroupStore,
                final Clock clock,
                final LinkedBlockingQueue<Pair<Integer, RolledUpActionStats>> rolledUpStats) {
            this.rollupNotificationSender = rollupNotificationSender;
            this.mgmtUnitSubgroupStore = mgmtUnitSubgroupStore;
            this.actionGroupStore = actionGroupStore;
            this.clock = clock;
            this.rolledUpStats = rolledUpStats;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    runIteration();
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        public void runIteration() throws InterruptedException {
            final List<Pair<Integer, RolledUpActionStats>> nextBatch = new ArrayList<>();
            final Pair<Integer, RolledUpActionStats> nextResult = rolledUpStats.poll(pollTimeoutMins, TimeUnit.MINUTES);
            if (nextResult == null) {
                // No results. This is expected, because the rollups we care about only happen hourly.
                logger.debug("Nothing to export after waiting for {} minutes", pollTimeoutMins);
            } else {
                // Drain the rest of the queued up stats into the batch.
                nextBatch.add(nextResult);
                rolledUpStats.drainTo(nextBatch);
                // Export.
                final Map<Integer, List<RolledUpActionStats>> statsByMgmtUnitSubgroup = nextBatch.stream()
                    .collect(Collectors.groupingBy(Pair::getLeft,
                        // It's unlikely, but it's possible that we get two entries for the same
                        // management unit subgroup.
                        Collectors.mapping(Pair::getRight, Collectors.toList())));
                try {
                    exportRollups(statsByMgmtUnitSubgroup);
                } catch (RuntimeException e) {
                    // In case of error we have no retry logic. The rollup will not run again, because
                    // we export rollups after they successfully save in the AO database.
                    // TODO: Log more here
                    logger.error("Failed to export stats for {} mgmt unit subgroups.",
                        statsByMgmtUnitSubgroup.keySet(), e);
                }
            }
        }

        @Nonnull
        private ActionRollupNotification createNotification(@Nonnull final Map<Integer, List<RolledUpActionStats>> statsByMgmtUnitSubgroup) {
            final Map<Integer, MgmtUnitSubgroup> mgmtUnitSubgroupsById =
                    mgmtUnitSubgroupStore.getGroupsById(statsByMgmtUnitSubgroup.keySet());
            final Map<Integer, ActionGroup> actionGroupsById = actionGroupStore.getGroupsById(
                statsByMgmtUnitSubgroup.values().stream()
                    .flatMap(Collection::stream)
                    .flatMap(stat -> stat.statsByActionGroupId().keySet().stream())
                    .collect(Collectors.toSet()));
            final ActionRollupNotification.Builder retBldr = ActionRollupNotification.newBuilder();

            // Add all the referenced action groups.
            actionGroupsById.forEach((actionGroupId, actionGroup) -> {
                retBldr.addActionGroups(ActionRollupExport.ActionGroup.newBuilder()
                    .setId(actionGroup.id())
                    .setActionType(actionGroup.key().getActionType())
                    .setActionCategory(actionGroup.key().getCategory())
                    .setActionSeverity(ActionView.categoryToSeverity(actionGroup.key().getCategory()))
                    .addAllRiskDescriptions(actionGroup.key().getActionRelatedRisk()));
            });

            // Add the stats in the batch.
            statsByMgmtUnitSubgroup.forEach((msuId, rolledUpActionStats) -> {
                MgmtUnitSubgroup mgmtUnitSubgroup = mgmtUnitSubgroupsById.get(msuId);
                if (mgmtUnitSubgroup == null) {
                    // Bad
                    return;
                }
                MgmtUnitSubgroupKey msuKey = mgmtUnitSubgroup.key();

                rolledUpActionStats.forEach(stat -> {
                    LocalDateTime startTime = stat.startTime();
                    ZonedDateTime zoneTime = startTime.atZone(clock.getZone());

                    final Map<Integer, RolledUpActionGroupStat> actionStatsMap = stat.statsByActionGroupId();
                    actionStatsMap.forEach((actionGroupId, rolledActionGroupStat) -> {
                        ActionGroup actionGroup = actionGroupsById.get(actionGroupId);
                        if (actionGroup != null) {
                            final HourlyActionStat.Builder statBldr = HourlyActionStat.newBuilder()
                                    .setHourTime(zoneTime.toInstant().toEpochMilli())
                                    .setScopeOid(mgmtUnitSubgroup.key().mgmtUnitId())
                                    .setActionGroupId(actionGroupId)
                                    .setPriorActionCount(rolledActionGroupStat.priorActionCount())
                                    .setNewActionCount(rolledActionGroupStat.newActionCount())
                                    .setEntityCount(stat.numActionSnapshots() * (int)rolledActionGroupStat.avgEntityCount())
                                    .setSavings(stat.numActionSnapshots() * (int)rolledActionGroupStat.avgSavings())
                                    .setInvestments(stat.numActionSnapshots() * (int)rolledActionGroupStat.avgInvestment());
                            if (msuKey.environmentType() != EnvironmentType.UNKNOWN_ENV) {
                                statBldr.setScopeEnvironmentType(msuKey.environmentType());
                            }
                            msuKey.entityType().ifPresent(statBldr::setScopeEntityType);

                            retBldr.addHourlyActionStats(statBldr);
                        }

                    });
                });
            });
            return retBldr.build();
        }

        private void exportRollups(@Nonnull final Map<Integer, List<RolledUpActionStats>> statsByMgmtUnitSubgroup) {
            try {
                final ActionRollupNotification notification = createNotification(statsByMgmtUnitSubgroup);
                rollupNotificationSender.sendMessage(notification);
            } catch (CommunicationException  ex) {
                logger.error("Cannot communicate with Kafka endpoint: ", ex);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                logger.error("Error while sending the rolledup action group statistics to Kafka: ", ex);
            }
        }
    }
}
