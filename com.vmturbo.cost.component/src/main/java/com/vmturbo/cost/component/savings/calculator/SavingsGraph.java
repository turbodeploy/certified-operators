package com.vmturbo.cost.component.savings.calculator;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.TierCostDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;

/**
 * A savings graph is a data structure used for savings calculation. It is a representation of a
 * time series line graph. There is a data point for each action of the action chain.
 *
 * <p>Each data point for a scale action holds the following values:
 * - Timestamp: time of the action
 * - High watermark: Let c(t) be the before-action cost. High watermark at t1 equals max(c(t)) where t <= t1.
 * - Low watermark: Let c(t) be the before-action cost. Low watermark at t1 equals min(c(t)) where t <= t1.
 * - Destination provider: The provider of the entity after the action.
 *
 * <p>Each data point for a delete action holds the following values:
 * - Timestamp: time of the action
 * - savingsPerHour: how much we can save per hour by executing the delete action
 *
 * <p>An action chain termination data point marks the end of an action chain. It only needs a
 * timestamp.
 */
public class SavingsGraph {
    private static final Logger logger = LogManager.getLogger();
    private final TreeSet<ActionDataPoint> dataPoints;
    private final long deleteActionExpiryMs;

    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    /**
     * Construct a watermark graph from a set of actions.
     *
     * @param actionChain action chain of an entity
     */
    public SavingsGraph(NavigableSet<ExecutedActionsChangeWindow> actionChain, long deleteActionExpiryMs) {
        dataPoints = new TreeSet<>(Comparator.comparingLong(ActionDataPoint::getTimestamp));
        this.deleteActionExpiryMs = deleteActionExpiryMs;
        populateDataPoints(actionChain);
    }

    private void populateDataPoints(NavigableSet<ExecutedActionsChangeWindow> actionChain) {
        double low = Double.MAX_VALUE;
        double high = Double.MIN_VALUE;

        for (ExecutedActionsChangeWindow changeWindow : actionChain) {
            final Action action = changeWindow.getActionSpec().getRecommendation();
            final long timestamp;
            if (changeWindow.getActionSpec().getExecutionStep().getStatus() == Status.SUCCESS) {
                timestamp = changeWindow.getActionSpec().getExecutionStep().getCompletionTime();
            } else {
                // It's not an executed action. Should not happen.
                continue;
            }
            if (action.getInfo().hasScale()) {
                final TierCostDetails sourceCostDetails = action.getInfo().getScale().getCloudSavingsDetails()
                        .getSourceTierCostDetails();

                // On-demand rate of the provider, get RI discounted rate if applicable.
                double sourceRate = sourceCostDetails.hasDiscountedRate()
                        ? sourceCostDetails.getDiscountedRate().getAmount()
                        : sourceCostDetails.getOnDemandRate().getAmount();

                low = Math.min(sourceRate, low);
                high = Math.max(sourceRate, high);

                long sourceProviderOid = 0L;
                long destProviderOid = 0L;
                if (action.getInfo().hasScale()) {
                    // If the action is used to change the provider, the source and destination provider
                    // OID will be the OID of the provider before and after the action respectively.
                    // If the action does not change the primary provider, but to change the commodity
                    // values only, both source and destination provider will be set to the primary provider OID.
                    sourceProviderOid = ActionDTOUtil.getPrimaryChangeProvider(action)
                            .map(changeProvider -> changeProvider.getSource().getId())
                            .orElse(ActionDTOUtil.getPrimaryProvider(action).map(ActionEntity::getId).orElse(0L));
                    destProviderOid = ActionDTOUtil.getPrimaryChangeProvider(action)
                            .map(changeProvider -> changeProvider.getDestination().getId())
                            .orElse(ActionDTOUtil.getPrimaryProvider(action).map(ActionEntity::getId).orElse(0L));
                }

                final TierCostDetails projectedTierCostDetails = action.getInfo().getScale()
                        .getCloudSavingsDetails().getProjectedTierCostDetails();
                boolean isRiCoverageExpectedAfterAction = projectedTierCostDetails
                        .getCloudCommitmentCoverage().hasCapacity();
                boolean isSavingsExpectedAfterAction = action.getSavingsPerHour().getAmount() > 0;
                double destinationOnDemandCost = projectedTierCostDetails.getOnDemandRate().getAmount();

                List<CommodityResize> commodityResizes = new ArrayList<>();
                if (action.getInfo().getScale().getCommodityResizesCount() > 0) {
                    action.getInfo().getScale().getCommodityResizesList().forEach(c ->
                        commodityResizes.add(new CommodityResize.Builder()
                                .oldCapacity(c.getOldCapacity())
                                .newCapacity(c.getNewCapacity())
                                .commodityType(c.getCommodityType().getType())
                                .build())
                    );
                }

                dataPoints.add(new ScaleActionDataPoint.Builder()
                        .timestamp(timestamp)
                        .lowWatermark(low)
                        .highWatermark(high)
                        .sourceProviderOid(sourceProviderOid)
                        .destinationProviderOid(destProviderOid)
                        .beforeActionCost(sourceRate)
                        .destinationOnDemandCost(destinationOnDemandCost)
                        .isCloudCommitmentExpectedAfterAction(isRiCoverageExpectedAfterAction)
                        .isSavingsExpectedAfterAction(isSavingsExpectedAfterAction)
                        .commodityResizes(commodityResizes)
                        .build());

                if (changeWindow.getLivenessState() == LivenessState.REVERTED
                        || changeWindow.getLivenessState() == LivenessState.EXTERNAL_MODIFICATION) {
                    dataPoints.add(new ActionChainTermination.Builder()
                            .timestamp(changeWindow.getEndTime())
                            .build());
                }
            } else if (action.getInfo().hasDelete()) {
                dataPoints.add(new DeleteActionDataPoint.Builder()
                        .timestamp(timestamp)
                        .savingsPerHour(action.getSavingsPerHour().getAmount())
                        .build());
                dataPoints.add(new ActionChainTermination.Builder()
                        .timestamp(timestamp + deleteActionExpiryMs)
                        .build());
            }
        }
    }

    /**
     * Get all data points of the watermark graph for a given day.
     *
     * @param datestamp the timestamp at the beginning of the day
     * @return a sorted set of watermark data points
     */
    public SortedSet<ActionDataPoint> getDataPointsInDay(long datestamp) {
        ActionDataPoint from = createWatermark(datestamp);
        ActionDataPoint to = createWatermark(datestamp + MILLIS_IN_DAY);
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
    public ActionDataPoint getDataPoint(long timestamp) {
        return dataPoints.floor(createWatermark(timestamp));
    }

    private ActionDataPoint createWatermark(long timestamp) {
        return new ActionDataPoint.Builder()
                .timestamp(timestamp)
                .destinationProviderOid(0)
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
        for (ActionDataPoint datapoint : dataPoints) {
            LocalDateTime time = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(datapoint.getTimestamp()), ZoneOffset.UTC);
            graph.append(time).append(": ").append(datapoint).append("\n");
        }
        return graph.toString();
    }
}
