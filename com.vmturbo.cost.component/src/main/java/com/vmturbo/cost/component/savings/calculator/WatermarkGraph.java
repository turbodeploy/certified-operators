package com.vmturbo.cost.component.savings.calculator;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.TierCostDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverage;

/**
 * A watermark graph is a data structure used for savings calculation. It is a representation of a
 * time series line graph. There is a data point for each action of the action chain. Each data point
 * holds the following values:
 * - Timestamp: time of the action
 * - High watermark: Let c(t) be the before-action cost. High watermark at t1 equals max(c(t)) where t <= t1.
 * - Low watermark: Let c(t) be the before-action cost. Low watermark at t1 equals min(c(t)) where t <= t1.
 * - Destination provider: The provider of the entity after the action.
 */
public class WatermarkGraph {
    private static final Logger logger = LogManager.getLogger();
    private final TreeSet<Watermark> dataPoints;

    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    /**
     * Construct a watermark graph from a set of actions.
     *
     * @param actionChain action chain of an entity
     */
    public WatermarkGraph(NavigableSet<ActionSpec> actionChain) {
        dataPoints = new TreeSet<>(Comparator.comparingLong(Watermark::getTimestamp));
        populateDataPoints(actionChain);
    }

    private void populateDataPoints(NavigableSet<ActionSpec> actionChain) {
        double low = Double.MAX_VALUE;
        double high = Double.MIN_VALUE;

        for (ActionSpec actionSpec : actionChain) {
            final Action action = actionSpec.getRecommendation();
            final long timestamp;
            if (actionSpec.getExecutionStep().getStatus() == Status.SUCCESS) {
                timestamp = actionSpec.getExecutionStep().getCompletionTime();
            } else {
                // It's not an executed action. Should not happen.
                continue;
            }
            // TODO Check volume rate is set in delete volume action.
            final TierCostDetails sourceCostDetails = action.getInfo().getScale().getCloudSavingsDetails()
                    .getSourceTierCostDetails();
            logger.trace("Source cost details of action {}: {}", action::getId, () -> sourceCostDetails);

            // On-demand rate of the provider
            double sourceRate = sourceCostDetails.getOnDemandRate().getAmount();
            // The rate after considering RI coverage
            if (sourceCostDetails.hasCloudCommitmentCoverage()) {
                final CloudCommitmentCoverage coverage = sourceCostDetails.getCloudCommitmentCoverage();
                final double used = coverage.getUsed().getCoupons();
                final double capacity = coverage.getCapacity().getCoupons();
                sourceRate = capacity == 0 ? sourceRate : sourceRate * (1 - used / capacity);
            }

            low = Math.min(sourceRate, low);
            high = Math.max(sourceRate, high);

            long destProviderOid = 0L;
            if (action.getInfo().hasScale()) {
                destProviderOid = ActionDTOUtil.getPrimaryChangeProvider(action)
                        .map(changeProvider -> changeProvider.getDestination().getId())
                        .orElse(0L);
            }

            dataPoints.add(new Watermark.Builder()
                    .timestamp(timestamp)
                    .lowWatermark(low)
                    .highWatermark(high)
                    .destinationProviderOid(destProviderOid)
                    .build());
        }
    }

    /**
     * Get all data points of the watermark graph for a given day.
     *
     * @param datestamp the timestamp at the beginning of the day
     * @return a sorted set of watermark data points
     */
    public SortedSet<Watermark> getDataPointsInDay(long datestamp) {
        Watermark from = createWatermark(datestamp);
        Watermark to = createWatermark(datestamp + MILLIS_IN_DAY);
        return dataPoints.subSet(from, to);
    }

    /**
     * Get the data point that is at or before a given timestamp.
     *
     * @param timestamp a timestamp
     * @return the data point that is at or before the timestamp. Return null if no data point at or
     *         before the given timestamp.
     */
    @Nullable
    public Watermark getWatermark(long timestamp) {
        return dataPoints.floor(createWatermark(timestamp));
    }

    private Watermark createWatermark(long timestamp) {
        return new Watermark.Builder()
                .timestamp(timestamp)
                .destinationProviderOid(0)
                .highWatermark(0)
                .lowWatermark(0)
                .build();
    }

    /**
     * Number of data points in the graph.
     *
     * @return number of data points in the graph
     */
    public int size() {
        return dataPoints.size();
    }

    @Override
    public String toString() {
        StringBuilder graph = new StringBuilder();
        graph.append("\n=== Watermark Graph (Number of data points: ").append(size()).append(") ===\n");
        for (Watermark datapoint : dataPoints) {
            LocalDateTime time = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(datapoint.getTimestamp()), ZoneOffset.UTC);
            graph.append(time);
            graph.append(": high=").append(datapoint.getHighWatermark());
            graph.append(", low=").append(datapoint.getLowWatermark());
            graph.append(", provider=").append(datapoint.getDestinationProviderOid());
            graph.append("\n");
        }
        return graph.toString();
    }
}
