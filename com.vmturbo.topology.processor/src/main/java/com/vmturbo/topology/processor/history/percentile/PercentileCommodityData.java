package com.vmturbo.topology.processor.history.percentile;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.UtilizationData;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Pre-calculated per-commodity field cache for percentile data.
 */
public class PercentileCommodityData
                implements IHistoryCommodityData<PercentileHistoricalEditorConfig, PercentileRecord, PercentileRecord.Builder> {
    private static final Logger logger = LogManager.getLogger();

    private UtilizationCountStore utilizationCounts;

    /**
     * Construct an empty instance of class.
     *
     * @apiNote We need an explicit empty constructor because this class has a throwing copy one.
     */
    public PercentileCommodityData() {
    }

    /**
     * Construct a copy of <code>other</code>.
     *
     * @param other object to copy from
     */
    public PercentileCommodityData(@Nonnull PercentileCommodityData other) {
        this.utilizationCounts = new UtilizationCountStore(other.utilizationCounts);
    }

    @Override
    public void init(@Nonnull EntityCommodityFieldReference field,
                     @Nullable PercentileRecord dbValue,
                     @Nonnull PercentileHistoricalEditorConfig config,
                     @Nonnull HistoryAggregationContext context) {
        try {
            if (utilizationCounts == null) {
                utilizationCounts = new UtilizationCountStore(
                                config.getPercentileBuckets(field.getCommodityType().getType()),
                                field);
                utilizationCounts.setPeriodDays(config.getObservationPeriod(context, field.getEntityOid()));
            }
            if (dbValue != null) {
                utilizationCounts.setLatestCountsRecord(dbValue);
            }
        } catch (HistoryCalculationException e) {
            logger.error("Failed to initialize percentile utilization storage for " + field, e);
        }
    }

    @Override
    public boolean needsReinitialization(@Nonnull EntityCommodityReference ref,
                    @Nonnull HistoryAggregationContext context,
                    @Nonnull PercentileHistoricalEditorConfig config) {
        int configuredPeriod = utilizationCounts.getPeriodDays();
        return configuredPeriod > 0 && configuredPeriod != config
                        .getObservationPeriod(context, ref.getEntityOid());
    }

    @Override
    public PercentileRecord.Builder checkpoint(@Nonnull List<PercentileRecord> outdated)
                    throws HistoryCalculationException {
        return utilizationCounts.checkpoint(outdated, true);
    }

    @Override
    public void aggregate(@Nonnull EntityCommodityFieldReference field,
                          @Nonnull PercentileHistoricalEditorConfig config,
                          @Nonnull HistoryAggregationContext context) {
        final boolean hasEnoughData = utilizationCounts.isMinHistoryDataAvailable(context, config);
        final ICommodityFieldAccessor commodityFieldsAccessor = context.getAccessor();
        if (!hasEnoughData) {
            logger.debug("Minimum amount of history data(for '{}' day(s)) is not available for '{}', so restriction policy will be applied",
                            () -> config.getMinObservationPeriod(context, field.getEntityOid()),
                            () -> field);
            commodityFieldsAccessor.applyInsufficientHistoricalDataPolicy(field);
        }
        Double capacity = commodityFieldsAccessor.getCapacity(field);
        if (capacity == null || capacity <= 0d) {
            logger.error("Cannot find capacity for commodity " + field
                         + ", no percentile will be calculated");
            return;
        }
        try {
            // do not update values in plan context, only set the result
            if (!context.isPlan()) {
                UtilizationData utilizationData = commodityFieldsAccessor.getUtilizationData(field);
                if (utilizationData != null) {
                    final int pointCount = utilizationData.getPointCount();
                    if (pointCount > 0) {
                        final long timestamp = utilizationData.getLastPointTimestampMs()
                                - utilizationData.getIntervalMs() * (pointCount - 1);
                        utilizationCounts.addPoints(utilizationData.getPointList(), capacity,
                                timestamp);
                    }
                } else {
                    // if this commodity is selected for percentile analysis, but mediation passed no data,
                    // generate a single point from real-time usage
                    Double used = commodityFieldsAccessor.getRealTimeValue(field);
                    if (used != null) {
                        utilizationCounts.addPoints(Collections.singletonList(used / capacity * 100), capacity,
                                                    config.getClock().millis());
                    }
                }
            }
            if (hasEnoughData) {
                // calculate and store the utilization into commodity's history value
                float aggressiveness = config.getAggressiveness(context, field.getEntityOid());
                final Integer percentile = utilizationCounts.getPercentile(aggressiveness);
                if (percentile != null) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Calculated percentile score for {} for rank {}: {}",
                            utilizationCounts, aggressiveness, percentile);
                    }
                    commodityFieldsAccessor.updateHistoryValue(field,
                        hv -> hv.setPercentile(percentile / 100d),
                        PercentileEditor.class.getSimpleName());
                } else {
                    logger.trace("No utilization counts recorded for {}", () -> field);
                }
            }
            commodityFieldsAccessor.clearUtilizationData(field);
        } catch (HistoryCalculationException e) {
            logger.error("Failed to aggregate percentile utilization for " + field, e);
        }
    }

    /**
     * Get the underlying utilization counts.
     *
     * @return utilization counts
     */
    public UtilizationCountStore getUtilizationCountStore() {
        return utilizationCounts;
    }
}
