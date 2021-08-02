package com.vmturbo.topology.processor.history.movingstats;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;
import com.vmturbo.topology.processor.history.InvalidHistoryDataException;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord;

/**
 * Pre-calculated per-commodity field cache for moving statistics data.
 */
public class MovingStatisticsCommodityData implements
    IHistoryCommodityData<MovingStatisticsHistoricalEditorConfig, MovingStatisticsRecord, MovingStatisticsRecord.Builder> {

    private MovingStatisticsSampler sampler;

    private static final String HISTORY_UPDATE_DESCRIPTION = "MovingStatisticsUpdateHistory";
    private static final String THRESHOLDS_UPDATE_DESCRIPTION = "MovingStatisticsUpdateThresholds";

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void aggregate(@Nonnull EntityCommodityFieldReference field,
                          @Nonnull MovingStatisticsHistoricalEditorConfig config,
                          @Nonnull HistoryAggregationContext context) {
        final ICommodityFieldAccessor commodityFieldsAccessor = context.getAccessor();
        final MovingStatisticsSamplingConfiguration<?> samplingConfig =
            config.getSamplingConfigurations(field.getCommodityType().getType());
        if (samplingConfig == null) {
            logger.error("Cannot find sampling configuration for commodity of type {}", field.getCommodityType());
            return;
        }

        // Process the commodity and its partner data together.
        final List<EntityCommodityFieldReference> partnerFields = samplingConfig.getPartnerCommodityTypes().stream()
            .map(partnerType -> new EntityCommodityFieldReference(field.getEntityOid(), partnerType, field.getField()))
            .collect(Collectors.toList());

        // Check against null capacity/used/lastUpdatedTime
        sampler.addSample(samplingConfig, field, partnerFields, commodityFieldsAccessor);

        final double standardDeviationsAbove = samplingConfig.getStandardDeviationsAbove();
        final double desiredStateTargetValue = samplingConfig.getDesiredStateTargetValue();

        updateHistoryAndThresholds(field, commodityFieldsAccessor,
            standardDeviationsAbove, desiredStateTargetValue);
        partnerFields.forEach(partner ->
            updateHistoryAndThresholds(partner, commodityFieldsAccessor,
                standardDeviationsAbove, desiredStateTargetValue));

        cleanExpiredData(config.getClock().millis(), samplingConfig);
    }

    @Override
    public void init(@Nonnull EntityCommodityFieldReference field,
                     @Nullable MovingStatisticsRecord movingStatisticsRecord,
                     @Nonnull MovingStatisticsHistoricalEditorConfig config,
                     @Nonnull HistoryAggregationContext context) {
        final MovingStatisticsSamplingConfiguration<?> samplingConfig =
            config.getSamplingConfigurations(field.getCommodityType().getType());
        if (samplingConfig == null) {
            logger.error("No sampling configuration for {}", field);
            return;
        }

        sampler = samplingConfig.supplySamplerInstance(field);
        if (movingStatisticsRecord != null) {
            try {
                sampler.deserialize(movingStatisticsRecord);
            } catch (InvalidHistoryDataException e) {
                logger.error("Failed to initialize moving statistics data for " + field, e);
            }
        }
    }

    @Override
    public boolean needsReinitialization(@Nonnull EntityCommodityReference ref,
                                         @Nonnull HistoryAggregationContext context,
                                         @Nonnull MovingStatisticsHistoricalEditorConfig movingStatisticsHistoricalEditorConfig) {
        return sampler == null;
    }

    /**
     * Serialize the commodity data to a protobuf blob.
     *
     * @return The builder for the serialized protobuf blob.
     * @throws InvalidHistoryDataException if the data is not properly initialized
     */
    public MovingStatisticsRecord.Builder serialize() throws InvalidHistoryDataException {
        if (sampler != null) {
            return sampler.serialize();
        } else {
            throw new InvalidHistoryDataException("Unable to serialize before initialization");

        }
    }

    /**
     * Deserialize the commodity data from a protobuf blob.
     *
     * @param record The record from which we should deserialize data into the internal sampled data.
     * @throws InvalidHistoryDataException If the data in the record is invalid or not properly initialized.
     */
    public void deserialize(@Nonnull MovingStatisticsRecord record) throws InvalidHistoryDataException {
        if (sampler != null) {
            sampler.deserialize(record);
        } else {
            throw new InvalidHistoryDataException("Unable to deserialize before initialization");
        }
    }

    /**
     * Get the associated sampler for this data.
     *
     * @return the associated sampler for this data.
     */
    protected MovingStatisticsSampler getSampler() {
        return sampler;
    }

    /**
     * Clean out any expired data (older than retention period).
     *
     * @param currentTimeMs The current time in milliseconds.
     * @param samplingConfig The associated configuration containing the retention period for the data.
     * @return true if any data was cleared, false if no data was cleared.
     */
    private boolean cleanExpiredData(long currentTimeMs,
                                     @Nonnull MovingStatisticsSamplingConfiguration<?> samplingConfig) {
        if (sampler != null) {
            return sampler.cleanExpiredData(currentTimeMs, samplingConfig);
        }

        return false;
    }

    private void updateHistoryAndThresholds(@Nonnull EntityCommodityFieldReference field,
                                            @Nonnull ICommodityFieldAccessor commodityFieldsAccessor,
                                            double analysisStandardDeviationsAbove,
                                            double desiredStateTargetValue) {
        final Double analysisValue = sampler.meanPlusSigma(field, analysisStandardDeviationsAbove);
        if (analysisValue != null) {
            // Set the value
            commodityFieldsAccessor.updateHistoryValue(field,
                hv -> hv.setMovingMeanPlusStandardDeviations(analysisValue), HISTORY_UPDATE_DESCRIPTION);

            final Double thresholdsValue = sampler.getMinThreshold(field,
                analysisStandardDeviationsAbove, desiredStateTargetValue);
            if (thresholdsValue != null) {
                // Set the thresholds
                commodityFieldsAccessor.updateThresholds(field, thresholdsUpdater(thresholdsValue),
                    THRESHOLDS_UPDATE_DESCRIPTION);
            } else {
                logger.trace("Skipping min threshold for {}", () -> field);
            }
        } else {
            logger.trace("Skipping movingMeanPlusStandardDeviations for {}", () -> field);
        }
    }

    private static Consumer<Thresholds.Builder> thresholdsUpdater(final double newMinThreshold) {
        return thresholds -> {
            double minThreshold = thresholds.hasMin()
                ? Math.max(thresholds.getMin(), newMinThreshold)
                : newMinThreshold;

            if (thresholds.hasMax()) {
                minThreshold = Math.min(minThreshold, thresholds.getMax());
            }
            // We only want to set the min threshold because we are trying to restrict resize down
            // actions and not up.
            thresholds.setMin(minThreshold);
        };
    }
}
