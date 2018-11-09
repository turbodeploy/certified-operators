package com.vmturbo.cost.calculation;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

/**
 * The {@link ReservedInstanceApplicator} is responsible for calculating the percentage of an
 * entity that's covered by reserved instances, and for recording the costs associated with those
 * reserved instances in the entity's {@link CostJournal}.
 *
 * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
 *                      {@link TopologyEntityDTO} for the realtime topology.
 */
public class ReservedInstanceApplicator<ENTITY_CLASS> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The journal to write the RI costs into.
     */
    private final CostJournal.Builder<ENTITY_CLASS> journal;

    /**
     * An extractor to get entity information out of the ENTITY_CLASS.
     */
    private final EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor;

    /**
     * The {@link CloudCostData} - most notably containing the RI coverage, as well as information
     * about bought RIs.
     */
    private final CloudCostData cloudCostData;

    /**
     * Use {@link ReservedInstanceApplicator#newFactory()} to get a factory to construct applicators.
     */
    private ReservedInstanceApplicator(@Nonnull final CostJournal.Builder<ENTITY_CLASS> journal,
                                       @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                                       @Nonnull final CloudCostData cloudCostData) {
        this.journal = journal;
        this.cloudCostData = cloudCostData;
        this.entityInfoExtractor = infoExtractor;
    }

    /**
     * Record the RI costs of the entity the associated journal is for into the journal.
     * We derive the RI costs from the RI coverage map obtained via the billing probe.
     *
     * Note: There is one {@link ReservedInstanceApplicator}. In the future, if we make the cost
     * journal global instead of per-entity, this method would take in an entity (or an entity ID).
     *
     * @param computeTier The compute tier from which the entity is buying. This signifies the
     *                    compute demand of the entity.
     * @return The percentage of the entity's compute demand that's covered by reserved instances.
     *         This should be a number between 0 and 1.
     */
    public double recordRICoverage(@Nonnull final ENTITY_CLASS computeTier) {
        final long entityId = entityInfoExtractor.getId(journal.getEntity());
        return cloudCostData.getRiCoverageForEntity(entityId)
            .map(entityRiCoverage -> {
                final double totalRequired = entityInfoExtractor.getComputeTierConfig(computeTier)
                    .orElseThrow(() -> new IllegalArgumentException("Expected compute tier with compute tier config."))
                    .getNumCoupons();
                double totalCovered = 0.0;
                for (Map.Entry<Long, Double> entry : entityRiCoverage.getCouponsCoveredByRiMap().entrySet()) {
                    final long riBoughtId = entry.getKey();
                    final double coveredCoupons = entry.getValue();
                    Optional<ReservedInstanceData> riDataOpt = cloudCostData.getRiBoughtData(riBoughtId);
                    if (riDataOpt.isPresent()) {
                        // Since we can calculate the cost, the coupons covered by this instance
                        // contribute to the RI coverage of the entity.
                        totalCovered += coveredCoupons;
                        final ReservedInstanceData riData = riDataOpt.get();

                        final double riBoughtPercentage = calculateRiBoughtPercentage(entityId,
                                coveredCoupons, riData);
                        final CurrencyAmount cost = CurrencyAmount.newBuilder()
                            .setCurrency(CostProtoUtil.getRiCurrency(riData.getReservedInstanceBought()))
                            .setAmount(calculateEffectiveHourlyCost(riBoughtPercentage, riData))
                            .build();
                        journal.recordRiCost(riData, coveredCoupons, cost);
                    } else {
                        // If we don't know about this reserved instance, we can't calculate a cost for
                        // it and we shouldn't include it in the cost calculation.
                        logger.error("Mismatched RI Coverage and RI Bought Store: " +
                                "No bought record for RI {}! Not including it in cost calculation", riBoughtId);
                    }
                }

                if (totalCovered == totalRequired) {
                    // Handle the equality case separately to avoid division by 0 if
                    // required == 0 and covered == 0.
                    return 1.0;
                } else if (totalCovered > totalRequired) {
                    logger.warn("Entity {} has RI coverage > 100%! Required: {} Covered: {}." +
                            " Trimming back to 100%.", entityId, totalRequired, totalCovered);
                    return 1.0;
                } else {
                    return totalCovered / totalRequired;
                }
            }).orElse(0.0);
    }

    /**
     * Calculate the EFFECTIVE hourly cost of coupons purchased by an entity from a reserved instance.
     * The effective hourly cost is the hourly usage cost + a portion of the upfront price, equivalent
     * to (upfront price) / (hours in term).
     *
     * @param riBoughtPercentage The percentage of the RI purchase to calculate the cost for. This
     *                      is essentially (# coupons purchased by entity) / (# coupons in RIBought)
     * @param riData {@link ReservedInstanceData} about the RI selling the coupons.
     * @return The effective hourly cost of the coupons.
     */
    private double calculateEffectiveHourlyCost(final double riBoughtPercentage,
                                                @Nonnull final ReservedInstanceData riData) {
        final ReservedInstanceBoughtCost cost = riData.getReservedInstanceBought()
                .getReservedInstanceBoughtInfo()
                .getReservedInstanceBoughtCost();

        // effective cost: (hourly cost) + (fixed cost / hours in term)
        final long hrsInTerm = CostProtoUtil.timeUnitsInTerm(
                riData.getReservedInstanceSpec().getReservedInstanceSpecInfo().getType(), TimeUnit.HOURS);
        // We currently don't consider the offering type of the RI Spec. This may be the wrong thing
        // to do. For example, if the RI Spec says the payment for the RI is all-upfront and the
        // RIBought has an hourly cost, it may be more correct to disregard the cost in the RIBought.
        // However, we take the RIBought to be authoritative.
        final double totalHourlyFixedCost = cost.getFixedCost().getAmount() / hrsInTerm;
        final double hourlyFixedCost = riBoughtPercentage * totalHourlyFixedCost;
        return hourlyFixedCost + calculateHourlyUsageCost(riBoughtPercentage, riData);
    }

    /**
     * Calculate the hourly cost of coupons purchased by an entity from a reserved instance.
     *
     * @param riBoughtPercentage The percentage of the RI purchase to calculate the cost for. This
     *                      is essentially (# coupons purchased by entity) / (# coupons in RIBought)
     * @param riData {@link ReservedInstanceData} about the RI selling the coupons.
     * @return The hourly cost of the coupons (not including the fixed cost).
     */
    private double calculateHourlyUsageCost(final double riBoughtPercentage,
                                            @Nonnull final ReservedInstanceData riData) {
        final ReservedInstanceBoughtCost cost = riData.getReservedInstanceBought()
                .getReservedInstanceBoughtInfo()
                .getReservedInstanceBoughtCost();

        // usage cost: recurring cost + usage cost.
        // TODO (roman, Sept 17 2018): Do we need to use entity state to determine whether to
        // take into account usage cost?
        return riBoughtPercentage * cost.getRecurringCostPerHour().getAmount() +
               riBoughtPercentage * cost.getUsageCostPerHour().getAmount();
    }

    /**
     * Calculate the percentage of an RI purchase that a certain number of coupons amounts to.
     * The number is equivalent to (coupons bought) / (coupons in RI purchase).
     *
     * @param entityId The ID of the entity buying the coupons (mainly for debugging/logging).
     * @param couponsBought The number of coupons the entity is buying from the RI.
     * @param riData {@link ReservedInstanceData} about the RI selling the coupons.
     * @return A number between 0 and 1.
     */
    private double calculateRiBoughtPercentage(final long entityId,
                                               final double couponsBought,
                                               @Nonnull final ReservedInstanceData riData) {
        final ReservedInstanceBoughtInfo riBoughtInfo =
                riData.getReservedInstanceBought().getReservedInstanceBoughtInfo();

        final int totalNumCoupons =
                riBoughtInfo.getReservedInstanceBoughtCoupons().getNumberOfCoupons();

        if (couponsBought > totalNumCoupons) {
            logger.warn("Inconsistent data - RI Bought {} is selling {} coupons, but entity {} is buying {}!",
                    riData.getReservedInstanceBought().getId(), totalNumCoupons, entityId, couponsBought);
            return 1;
        } else if (couponsBought == 0) {
            return 0;
        } else {
            return couponsBought / totalNumCoupons;
        }
    }

    @Nonnull
    public static <ENTITY_CLASS> ReservedInstanceApplicatorFactory<ENTITY_CLASS> newFactory() {
        return ReservedInstanceApplicator::new;
    }

    /**
     * A factory class for {@link ReservedInstanceApplicator} instances, used for dependency
     * injection and mocking for tests.
     *
     * @param <ENTITY_CLASS> See {@link ReservedInstanceApplicator}.
     */
    @FunctionalInterface
    public interface ReservedInstanceApplicatorFactory<ENTITY_CLASS> {

        @Nonnull
        ReservedInstanceApplicator<ENTITY_CLASS> newReservedInstanceApplicator(
                @Nonnull final CostJournal.Builder<ENTITY_CLASS> journal,
                @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                @Nonnull final CloudCostData cloudCostData);

    }
}
