package com.vmturbo.topology.processor.history.movingstats;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.exceptions.InvalidHistoryDataException;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord;

/**
 * Maintains moving statistics for a set of commodities (one principal commodity, and a collection of
 * partner commodities), permitting the joint update of the history of multiple commodities on a single
 * entity. Note that as of now, analysis values are fetched for the principal and all partner commodities,
 * however the minThreshold is only fetched for the principal commodity.
 * <p/>
 * The exact moving statistics maintained for a particular commodity or group of commodities is up to
 * the implementation, but generally we keep moving average and standard deviation for two sets of
 * moving statistics - fast and slow. The fast moving statistics are maintained to be able to quickly
 * respond to spikes in usage. The slow moving statistics are maintained to be able to ensure a
 * sufficiently long memory for high usage that may have occurred in the past.
 */
public interface MovingStatisticsSampler {
    /**
     * Add a new sample to the {@link MovingStatisticsSampler}.
     *
     * @param configuration Configuration describing how the statistics should be sampled.
     * @param principalCommodityField The {@link EntityCommodityFieldReference} for the principal commodity. See
     *                                {@link MovingStatisticsSamplingConfiguration#getPartnerCommodityTypes()}
     *                                for more details.
     * @param partnerCommodityFields The commodity fields in this list are ordered in the same order that partner
     *                              commodities are listed in the
     *                              {@link MovingStatisticsSamplingConfiguration#getPartnerCommodityTypes()}.
     *                              Note that values in this list may be if the entity selling the principal
     *                              commodity is not selling all the partner commodity entity types. It is up
     *                              to the implementation to handle null values appropriately.
     * @param commodityFieldAccessor {@link ICommodityFieldAccessor} permitting lookup of the values for the
     *                               commodity fields.
     */
    void addSample(@Nonnull MovingStatisticsSamplingConfiguration<?> configuration,
                   @Nonnull EntityCommodityFieldReference principalCommodityField,
                   @Nonnull List<EntityCommodityFieldReference> partnerCommodityFields,
                   @Nonnull ICommodityFieldAccessor commodityFieldAccessor);

    /**
     * Get the analysis value for a given commodity. This will be called for the principal commodity
     * and all partner commodities. This value will be set in the
     * TopologyEntityDTO#historicalUsed#movingMeanPlusSigma field.
     *
     * @param field The field for the commodity whose analysis value should be calculated.
     * @param standardDeviationsAboveMean The number of standard deviations above the mean to use for the
     *                                    analysis value.
     * @return Compute an analysis value equal to:
     *         {@code movingAverage + standardDeviationsAboveMean * movingStdDev}
     */
    @Nullable
    Double meanPlusSigma(@Nonnull EntityCommodityFieldReference field, double standardDeviationsAboveMean);

    /**
     * Get the value to set on the commodity's Thresholds#min field (this translates to the capacity
     * lower bound on the commodity in the market).
     *
     * @param field The field for the commodity whose min threshold should be computed.
     * @param standardDeviationsAboveMean The number of standard deviations above the mean to consider
     *                                    when computing the minThreshold.
     * @param desiredStateTarget The target desired state for this commodity. Usually we target a desired
     *                           state of 70% utilization, but some commodities (ie throttling) may adjust
     *                           this number if optimal performance is not achieved at 70%.
     * @return The minimum threshold for the given commodity field.
     */
    @Nullable
    Double getMinThreshold(@Nonnull EntityCommodityFieldReference field,
                           double standardDeviationsAboveMean, double desiredStateTarget);

    /**
     * Clear out any expired data from the sampler.
     *
     * @param currentTimeMs The current time in milliseconds.
     * @param configuration The configuration for the sampler. The configuration contains
     *                      the retention period duration.
     * @return Whether any data was cleared.
     */
    boolean cleanExpiredData(long currentTimeMs, @Nonnull MovingStatisticsSamplingConfiguration<?> configuration);

    /**
     * Serialize the sampler data to a {@link MovingStatisticsRecord.Builder}.
     *
     * @return the serialized data from this sampler.
     */
    @Nonnull
    MovingStatisticsRecord.Builder serialize();

    /**
     * Deserialize data from the {@link MovingStatisticsRecord}.
     *
     * @param record The record to deserialize into this sampler.
     * @throws InvalidHistoryDataException If the data are in an invalid format.
     */
    void deserialize(@Nonnull MovingStatisticsRecord record) throws InvalidHistoryDataException;
}
