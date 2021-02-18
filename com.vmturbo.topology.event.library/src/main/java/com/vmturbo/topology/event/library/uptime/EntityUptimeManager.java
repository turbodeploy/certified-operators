package com.vmturbo.topology.event.library.uptime;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.stringtemplate.v4.ST;

import com.vmturbo.cloud.common.data.BoundedDuration;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.data.stats.DurationStatistics;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.event.library.TopologyEventProvider;
import com.vmturbo.topology.event.library.TopologyEventProvider.TopologyEventFilter;
import com.vmturbo.topology.event.library.TopologyEventProvider.TopologyEventUpdateListener;
import com.vmturbo.topology.event.library.TopologyEvents;
import com.vmturbo.topology.event.library.uptime.EntityUptimeCalculator.EntityUptimeCalculation;

/**
 * The entity uptime manager is responsible for scheduling topology uptime calculations and persisting
 * the uptime to the {@link EntityUptimeStore}.
 */
public class EntityUptimeManager implements TopologyEventUpdateListener {

    private static final String UPTIME_CALCULATION_SUMMARY =
            "Entities: <entityCount>\n"
                    + "Uptime Duration: <uptimeDuration>\n"
                    + "Total Duration: <totalDuration>\n"
                    + "Uptime Percentage: <uptimePercentage>\n";

    private static final DataMetricSummary TOTAL_UPTIME_CALCULATION_DURATION_SUMMARY =
            DataMetricSummary.builder()
                    .withName("entity_uptime_calculation_duration_seconds")
                    .withHelp("Total time for entity uptime calculation. ")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(6) // 6 buckets, so buckets get switched every 10 minutes.
                    .build()
                    .register();

    private final Logger logger = LogManager.getLogger();

    private final TopologyEventProvider eventProvider;

    private final EntityUptimeCalculator uptimeCalculator;

    private final EntityUptimeStore entityUptimeStore;

    private final Duration windowDuration;

    private final BoundedDuration calculationInterval;

    private final boolean cacheUptimeCalculations;

    private final boolean isUptimeEnabled;

    private final Object uptimeCalculationLock = new Object();

    private Map<Long, CachedEntityUptime> cachedUptimeMap = Collections.EMPTY_MAP;

    /**
     * Constructs a new {@link EntityUptimeManager} instance.
     * @param eventProvider The topology event provider.
     * @param uptimeCalculator The entity uptime calculator.
     * @param entityUptimeStore The entity uptime store.
     * @param windowDuration The duration of time to calculate the uptime percentage over. Typically,
     *                       this will be 730 hours (1 cloud month).
     * @param calculationInterval How often (hourly, daily) and with what delay (e.g. 1 hour lag time)
     *                            the uptime should be calculated.
     * @param cacheUptimeCalculations Whether uptime calculations should be cached, in order to only
     *                                calculate the net new uptime on each successive topology uptime
     *                                calculation.
     * @param isUptimeEnabled Indicates whether topology uptime should be calculated.
     */
    public EntityUptimeManager(@Nonnull TopologyEventProvider eventProvider,
                               @Nonnull EntityUptimeCalculator uptimeCalculator,
                               @Nonnull EntityUptimeStore entityUptimeStore,
                               @Nonnull Duration windowDuration,
                               @Nonnull BoundedDuration calculationInterval,
                               boolean cacheUptimeCalculations,
                               boolean isUptimeEnabled) {

        this.eventProvider = Objects.requireNonNull(eventProvider);
        this.uptimeCalculator = Objects.requireNonNull(uptimeCalculator);
        this.entityUptimeStore = Objects.requireNonNull(entityUptimeStore);
        this.windowDuration = Objects.requireNonNull(windowDuration);
        this.calculationInterval = Objects.requireNonNull(calculationInterval);
        this.cacheUptimeCalculations = cacheUptimeCalculations;
        this.isUptimeEnabled = isUptimeEnabled;

        eventProvider.registerUpdateListener(this);
    }

    /**
     * The entity uptime manager listens for any updates to the topology event data set. The current
     * notification (given it's based on CCA demand) is triggered on persistence after every topology
     * broadcast. The entity uptime manager is using this guarantee to schedule rolling the entity
     * uptime window forward. Once topology events are discovered data, the manager will need a separate
     * trigger for rolling the uptime window.
     */
    @Override
    public void onTopologyEventUpdate() {

        logger.info("Received topology event update notification. Checking for uptime recalculation");

        updateUptimeForTopology(false);
    }

    /**
     * Calculates the topology uptime (uptime for each cloud workload in the topology). If not forced,
     * a check will be performed on the uptime window to see if it has progressed. If the current uptime
     * window matches the prior calculation, no uptime calculation will occur. If forced, a full topology
     * calculation will be performed.
     * @param forceRecalculation If true, a full topology calculation will be performed. If false,
     *                           a full or cached calculation may occur, based on the prior uptime
     *                           window.
     */
    public void updateUptimeForTopology(boolean forceRecalculation) {

        synchronized (uptimeCalculationLock) {

            final TimeInterval currentUptimeWindow = entityUptimeStore.getUptimeWindow();
            final Instant targetEndTime = Instant.now()
                    .truncatedTo(calculationInterval.unit())
                    .minus(calculationInterval.duration());

            if (isUptimeEnabled
                    && (currentUptimeWindow.endTime().isBefore(targetEndTime) || forceRecalculation)) {

                try (DataMetricTimer timer = TOTAL_UPTIME_CALCULATION_DURATION_SUMMARY.startTimer()) {
                    final TimeInterval newUptimeWindow = TimeInterval.builder()
                            .startTime(targetEndTime.minus(windowDuration))
                            .endTime(targetEndTime)
                            .build();

                    logger.info("Updating entity uptime (Current Uptime Window={}, New Uptime Window={})",
                            currentUptimeWindow, newUptimeWindow);

                    final Map<Long, EntityUptime> entityUptimeMap;
                    final Map<Long, CachedEntityUptime> cachedUptimeMap;
                    if (cacheUptimeCalculations && !forceRecalculation) {
                        throw new UnsupportedOperationException();
                    } else {
                        entityUptimeMap = calculateFullUptime(newUptimeWindow);
                        cachedUptimeMap = Collections.EMPTY_MAP;
                    }

                    entityUptimeStore.persistTopologyUptime(newUptimeWindow, entityUptimeMap);
                    this.cachedUptimeMap = cachedUptimeMap;

                    logger.info("Finished {} entity uptime calculations in {} seconds",
                            entityUptimeMap.size(), timer.getTimeElapsedSecs());

                    summarizeEntityUptime(entityUptimeMap);

                } catch (Exception e) {
                    logger.error("Error calculating entity uptime", e);
                }
            } else {
                if (isUptimeEnabled) {
                    logger.debug("Skipping entity uptime update (Current Uptime Window={}, Target End Time={})",
                            currentUptimeWindow, targetEndTime);
                } else {
                    logger.info("Skipping uptime calculation. Entity uptime is disabled.");
                }
            }
        }
    }

    private void summarizeEntityUptime(@Nonnull Map<Long, EntityUptime> entityUptimeMap) {

        final DurationStatistics.Collector uptimeCollector = DurationStatistics.collector();
        final DurationStatistics.Collector totalCollector = DurationStatistics.collector();
        final DoubleSummaryStatistics percentageStats = new DoubleSummaryStatistics();

        entityUptimeMap.values().forEach(entityUptime -> {
            uptimeCollector.collect(entityUptime.uptime());
            totalCollector.collect(entityUptime.totalTime());
            percentageStats.accept(entityUptime.uptimePercentage());
        });

        final ST template = new ST(UPTIME_CALCULATION_SUMMARY);

        template.add("entityCount", entityUptimeMap.size());
        template.add("uptimeDuration", uptimeCollector.toStatistics());
        template.add("totalDuration", totalCollector.toStatistics());
        template.add("uptimePercentage", percentageStats);

        logger.info("Entity uptime summary:\n{}", template.render());
    }

    private Map<Long, EntityUptime> calculateFullUptime(@Nonnull TimeInterval uptimeWindow) {

        // Pull in all topology event records - we want to pull in events prior to the uptime window
        // to be able to infer the power state at the start of the uptime window.
        final TimeInterval eventWindow = TimeInterval.builder()
                .startTime(Instant.EPOCH)
                .endTime(Instant.now())
                .build();
        final TopologyEvents topologyEvents = eventProvider.getTopologyEvents(
                eventWindow, TopologyEventFilter.ALL);

        return topologyEvents.ledgers().values().stream()
                .map(eventLedger -> {
                    try {
                        return uptimeCalculator.calculateUptime(eventLedger, uptimeWindow);
                    } catch (Exception e) {
                        logger.error("Error calculating entity uptime for entity '{}'",
                                eventLedger.entityOid(), e);
                        return null;
                    }
                }).filter(Objects::nonNull)
                .collect(ImmutableMap.toImmutableMap(
                        EntityUptimeCalculation::entityOid,
                        EntityUptimeCalculation::entityUptime));
    }
}
