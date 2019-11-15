package com.vmturbo.topology.processor.history.percentile;

import java.util.Collections;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.UtilizationData;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Pre-calculated per-commodity field cache for percentile data.
 */
public class PercentileCommodityData
                implements IHistoryCommodityData<PercentileHistoricalEditorConfig, PercentileRecord> {
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
     * @throws HistoryCalculationException when coping of utilizationCounts fails
     */
    public PercentileCommodityData(PercentileCommodityData other)
                    throws HistoryCalculationException {
        this.utilizationCounts = new UtilizationCountStore(other.utilizationCounts);
    }

    @Override
    public void init(@Nonnull EntityCommodityFieldReference field,
                     @Nullable PercentileRecord dbValue,
                     @Nonnull PercentileHistoricalEditorConfig config,
                     @Nonnull ICommodityFieldAccessor commodityFieldsAccessor) {
        try {
            if (utilizationCounts == null) {
                utilizationCounts = new UtilizationCountStore(config
                                .getPercentileBuckets(field.getCommodityType().getType()), field);
                utilizationCounts.setPeriodDays(config.getObservationPeriod(field.getEntityOid()));
            }
            if (dbValue != null) {
                utilizationCounts.setLatestCountsRecord(dbValue);
            }
        } catch (HistoryCalculationException e) {
            logger.error("Failed to initialize percentile utilization storage for " + field, e);
        }
    }

    @Override
    public void aggregate(@Nonnull EntityCommodityFieldReference field,
                          @Nonnull PercentileHistoricalEditorConfig config,
                          @Nonnull ICommodityFieldAccessor commodityFieldsAccessor) {
        Double capacity = commodityFieldsAccessor.getCapacity(field);
        if (capacity == null || capacity <= 0d) {
            logger.error("Cannot find capacity for commodity " + field
                         + ", no percentile will be calculated");
            return;
        }
        try {
            // do not update values in plan context, only set the result
            if (!config.isPlan()) {
                UtilizationData utilizationData = commodityFieldsAccessor.getUtilizationData(field);
                if (utilizationData != null) {
                    utilizationCounts.addPoints(utilizationData.getPointList(), capacity,
                                                utilizationData.getLastPointTimestampMs() - utilizationData
                                                                .getIntervalMs() * (utilizationData.getPointCount() - 1));
                } else {
                    // if this commodity is selected for percentile analysis, but mediation passed no data,
                    // generate a single point from real-time usage
                    Double used = commodityFieldsAccessor.getRealTimeValue(field);
                    if (used != null) {
                        utilizationCounts.addPoints(Collections.singletonList(used / capacity * 100), capacity,
                                                    System.currentTimeMillis());
                    }
                }
            }

            // calculate and store the utilization into commodity's history value
            int aggressiveness = config.getAggressiveness(field.getEntityOid());
            int percentile = utilizationCounts.getPercentile(aggressiveness);
            if (logger.isTraceEnabled()) {
                logger.trace("Calculated percentile score for {} for rank {}: {}",
                        utilizationCounts, aggressiveness, percentile);
            }
            commodityFieldsAccessor.updateHistoryValue(field,
                                                       hv -> hv.setPercentile(percentile / 100d),
                                                       PercentileEditor.class.getSimpleName());
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
