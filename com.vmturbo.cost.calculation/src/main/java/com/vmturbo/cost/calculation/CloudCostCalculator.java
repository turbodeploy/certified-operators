package com.vmturbo.cost.calculation;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CostJournal.CostCategory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;

/**
 * This is the main entry point into the cost calculation library. The user is responsible for
 * providing implementations of the integration classes to the constructor.
 *
 * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
 *                      {@link TopologyEntityDTO} for the realtime topology.
 */
public class CloudCostCalculator<ENTITY_CLASS> {

    private static final Logger logger = LogManager.getLogger();

    private final CloudCostData cloudCostData;

    private final CloudTopology<ENTITY_CLASS> cloudTopology;

    private final EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor;

    public CloudCostCalculator(@Nonnull final CloudCostDataProvider cloudCostDataProvider,
                               @Nonnull final CloudTopology<ENTITY_CLASS> cloudTopology,
                               @Nonnull final EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor)
            throws CloudCostDataRetrievalException {
        this.cloudCostData = Objects.requireNonNull(cloudCostDataProvider).getCloudCostData();
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.entityInfoExtractor = Objects.requireNonNull(entityInfoExtractor);
    }

    /**
     * Calculate the cost for a single entity. Since the calculation is done in-memory, and there
     * are no dependencies between entities w.r.t. cost at the time of this writing, there is no
     * bulk operation to calculate cost for an entire topology.
     *
     * An example - suppose a VM is using a m4.large template which costs $1/hr, and is half-covered
     * by an RI that costs $0.25/hr. It also has two disks, buying 1GB from storage tiers at
     * $1/gb/mo and $2/gb/mo respectively. It's owned by a business account that has
     * a 10% global discount. The cost should therefore be:
     *
     * ($1 * 0.5
     * + $0.25 (? not sure if we need to multiply by 0.5 here, or if that's factored
     * into the RI price)
     * + 1 * $1
     * + 1 * $2) * 0.9 (applying the discount)
     *
     * (TODO (roman, Aug 16 2018): there may be dependencies when we add storage volumes, because
     * storage volumes will buy from storage tiers, and VMs will buy from storage volumes. In that
     * case we will want to process the volumes first)
     *
     * @param entity The entity to calculate cost for.
     * @return A {@link CostJournal} with the cost breakdown. Use methods on the cost journal
     *         to get the actual costs.
     */
    @Nonnull
    public CostJournal<ENTITY_CLASS> calculateCost(@Nonnull final ENTITY_CLASS entity) {
        if (entityInfoExtractor.getEntityType(entity) != EntityType.VIRTUAL_MACHINE_VALUE) {
            // Not supporting cost calculation for anything other than VMs for now.
            return CostJournal.empty();
        }

        final long entityId = entityInfoExtractor.getId(entity);
        final Optional<ENTITY_CLASS> regionOpt = cloudTopology.getRegion(entityId);
        if (!regionOpt.isPresent()) {
            logger.warn("Unable to find region for entity {}. Returning empty cost.", entityId);
            return CostJournal.empty();
        }
        final ENTITY_CLASS region = regionOpt.get();

        final CostJournal.Builder<ENTITY_CLASS> journal = CostJournal.newBuilder(entity, region);

        entityInfoExtractor.getComputeConfig(entity).ifPresent(computeConfig -> {
            // Calculate on-demand prices for entities that have a compute config.
            cloudTopology.getComputeTier(entityId).ifPresent(computeTier -> {
                final long regionId = entityInfoExtractor.getId(region);
                final OnDemandPriceTable onDemandPriceTable = cloudCostData.getPriceTable()
                    .getOnDemandPriceByRegionIdMap().get(regionId);
                if (onDemandPriceTable != null) {
                    final ComputeTierPriceList computePriceList =
                        onDemandPriceTable.getComputePricesByTierIdMap()
                            .get(entityInfoExtractor.getId(computeTier));
                    if (computePriceList != null) {
                        final ComputeTierConfigPrice basePrice = computePriceList.getBasePrice();
                        // TODO (roman, Aug 17 2018): When implementing RI's, amount bought will
                        // be the fraction of the VM that's bought at on-demand prices.
                        final double amountBought = 1;
                        journal.recordOnDemandCost(CostCategory.COMPUTE, computeTier,
                            basePrice.getPricesList().get(0), amountBought);
                        if (computeConfig.getOs() != basePrice.getGuestOsType()) {
                            computePriceList.getPerConfigurationPriceAdjustmentsList().stream()
                                .filter(computeConfig::matchesPriceTableConfig)
                                .findAny()
                                .ifPresent(priceAdjustmentConfig -> journal.recordOnDemandCost(
                                    CostCategory.LICENSE,
                                    computeTier,
                                    priceAdjustmentConfig.getPricesList().get(0), amountBought));
                        }
                    }
                } else {
                    logger.warn("Global price table has no entry for region {}. This means there" +
                        " is some inconsistency between the topology and pricing data.", regionId);
                }
            });
        });

        return journal.build();
    }
}
