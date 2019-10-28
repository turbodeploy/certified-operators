package com.vmturbo.cost.calculation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Pricing;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.LicensePriceTuple;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.DatabaseConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.VirtualVolumeConfig;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LicenseModel;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList.DatabaseTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;

/**
 * This is the main entry point into the cost calculation library. The user is responsible for
 * providing implementations of the integration classes to the constructor.
 *
 * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
 *                      {@link TopologyEntityDTO} for the realtime topology.
 */
public class CloudCostCalculator<ENTITY_CLASS> {

    private static final Logger logger = LogManager.getLogger();

    private static final Set<Integer> ENTITY_TYPES_WITH_COST = ImmutableSet.of(
                                        EntityType.VIRTUAL_MACHINE_VALUE,
                                        EntityType.DATABASE_SERVER_VALUE,
                                        EntityType.DATABASE_VALUE,
                                        EntityType.VIRTUAL_VOLUME_VALUE);

    private final CloudCostData cloudCostData;

    private final CloudTopology<ENTITY_CLASS> cloudTopology;

    private final EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor;

    private final DiscountApplicatorFactory<ENTITY_CLASS> discountApplicatorFactory;

    private final ReservedInstanceApplicatorFactory<ENTITY_CLASS> reservedInstanceApplicatorFactory;

    private final DependentCostLookup<ENTITY_CLASS> dependentCostLookup;

    private final Map<Long, EntityReservedInstanceCoverage> topologyRICoverage;

    private CloudCostCalculator(@Nonnull final CloudCostData cloudCostData,
               @Nonnull final CloudTopology<ENTITY_CLASS> cloudTopology,
               @Nonnull final EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor,
               @Nonnull final DiscountApplicatorFactory<ENTITY_CLASS> discountApplicatorFactory,
               @Nonnull final ReservedInstanceApplicatorFactory<ENTITY_CLASS> reservedInstanceApplicatorFactory,
               @Nonnull final DependentCostLookup<ENTITY_CLASS> dependentCostLookup,
               @Nonnull final Map<Long, EntityReservedInstanceCoverage> topologyRICoverage) {
        this.cloudCostData = Objects.requireNonNull(cloudCostData);
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.entityInfoExtractor = Objects.requireNonNull(entityInfoExtractor);
        this.discountApplicatorFactory = Objects.requireNonNull(discountApplicatorFactory);
        this.reservedInstanceApplicatorFactory = Objects.requireNonNull(reservedInstanceApplicatorFactory);
        this.dependentCostLookup = Objects.requireNonNull(dependentCostLookup);
        this.topologyRICoverage = Objects.requireNonNull(topologyRICoverage);
    }

    /**
     * A function to look up calculated costs for entities.
     *
     * We use this to handle dependencies between costs. If the cost for a single entity depends
     * on the costs of other entities, we use this function (after all cost calculations are
     * completed) to look up the dependency's cost.
     *
     * @param <ENTITY_CLASS> See {@link CloudCostCalculator}.
     */
    @FunctionalInterface
    public interface DependentCostLookup<ENTITY_CLASS> {

        /**
         * Get the cost journal for a particular entity.
         *
         * @param entity The entity to look up the journal for.
         * @return The cost journal, or null if there is no journal for the entity.
         */
        @Nullable
        CostJournal<ENTITY_CLASS> getCostJournal(@Nonnull final ENTITY_CLASS entity);
    }

    /**
     * Calculate the cost for a single entity.
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
     * @param entity The entity to calculate cost for.
     * @return A {@link CostJournal} with the cost breakdown. Use methods on the cost journal
     *         to get the actual costs.
     */
    @Nonnull
    public CostJournal<ENTITY_CLASS> calculateCost(@Nonnull final ENTITY_CLASS entity) {
        final long entityId = entityInfoExtractor.getId(entity);
        if (!ENTITY_TYPES_WITH_COST.contains(entityInfoExtractor.getEntityType(entity))) {
            logger.debug("Skipping cost calculation for entity {} due to unsupported entity type {}",
                entityId, entityInfoExtractor.getEntityType(entity));
            return CostJournal.empty(entity, entityInfoExtractor);
        }

        final Optional<ENTITY_CLASS> regionOpt = cloudTopology.getConnectedRegion(entityId);
        if (!regionOpt.isPresent()) {
            logger.warn("Unable to find region for entity {}. Returning empty cost.", entityId);
            return CostJournal.empty(entity, entityInfoExtractor);
        }
        final ENTITY_CLASS region = regionOpt.get();

        final DiscountApplicator<ENTITY_CLASS> discountApplicator =
                discountApplicatorFactory.entityDiscountApplicator(entity, cloudTopology, entityInfoExtractor, cloudCostData);

        final CostJournal.Builder<ENTITY_CLASS> journal =
                CostJournal.newBuilder(entity, entityInfoExtractor, region, discountApplicator, dependentCostLookup);
        switch (entityInfoExtractor.getEntityType(entity)) {
            case EntityType.VIRTUAL_MACHINE_VALUE:
                calculateVirtualMachineCost(entity, region, journal);
                break;
            case EntityType.DATABASE_VALUE:
                calculateDatabaseCost(entity, region, false, journal);
                break;
            case EntityType.DATABASE_SERVER_VALUE:
                calculateDatabaseCost(entity, region, true, journal);
                break;
            case EntityType.VIRTUAL_VOLUME_VALUE:
                calculateVirtualVolumeCost(entity, region, journal);
                break;
            default:
                logger.error("Received invalid entity " + entity.toString());
                break;
        }

        return journal.build();
    }

    private void calculateVirtualVolumeCost(@Nonnull final ENTITY_CLASS entity,
                @Nonnull final ENTITY_CLASS region,
                @Nonnull final CostJournal.Builder<ENTITY_CLASS> journal) {
        final long entityId = entityInfoExtractor.getId(entity);
        logger.trace("Starting entity cost calculation for volume {}", entityId);
        final Optional<VirtualVolumeConfig> volumeConfigOpt = entityInfoExtractor.getVolumeConfig(entity);
        if (volumeConfigOpt.isPresent()) {
            final VirtualVolumeConfig volumeConfig = volumeConfigOpt.get();
            final Optional<ENTITY_CLASS> storageTierOpt = cloudTopology.getStorageTier(entityId);
            if (storageTierOpt.isPresent()) {
                final ENTITY_CLASS storageTier = storageTierOpt.get();
                final long regionId = entityInfoExtractor.getId(region);
                final long storageTierId = entityInfoExtractor.getId(storageTier);
                final OnDemandPriceTable onDemandPriceTable =
                    cloudCostData.getPriceTable().getOnDemandPriceByRegionIdMap().get(regionId);
                if (onDemandPriceTable != null) {
                    final StorageTierPriceList storageTierPrices =
                            onDemandPriceTable.getCloudStoragePricesByTierIdMap().get(storageTierId);
                    if (storageTierPrices != null) {
                        // Volumes are charged based on their IOPS and GB capacity.
                        // Different storage tiers may price this differently - some tiers charge you
                        // for what you use, and some tiers have price "levels" depending on the size
                        // of the disk.
                        //
                        // Extract the different prices present in this tier's price list. This will
                        // tell us how to price the capacity of the volume.
                        //
                        // TODO (roman, 17 Oct 2018):
                        // It may make sense to put price ranges into individual messages, so that instead
                        // of "repeated Price" we have "repeated TieredPrice". That way we don't need
                        // to do this grouping + sorting during calculation.
                        final Map<Price.Unit, List<Price>> pricesByUnit =
                            createSortedStoragePriceMap(storageTierPrices);
                        recordStorageRangePricesByUnit(pricesByUnit, journal, storageTier,
                            volumeConfig.getAccessCapacityMillionIops(), Unit.MILLION_IOPS);
                        recordStorageRangePricesByUnit(pricesByUnit, journal, storageTier,
                            volumeConfig.getAmountCapacityGb(), Unit.GB_MONTH);
                        recordRangePricesForMonth(pricesByUnit.get(Unit.MONTH),
                            volumeConfig.getAmountCapacityGb(), journal, storageTier);
                    } else {
                        logger.error("Could not calculate cost for Virtual volume {}. Price table " +
                                "for region {} has no entry for tier {}. Skipping cost " +
                                "calculation.", entityId, regionId, storageTierId);
                    }
                } else {
                    logger.error("calculateVirtualVolumeCost: Global price table has no entry for region {}." +
                            "  This means there is some inconsistency between the topology and pricing data.", regionId);
                }
            } else {
                logger.error("Unable to find related storage tier for volume entity {}. " +
                    "Skipping cost calculation.", entityId);
            }
        } else {
            logger.error("No volume config present for volume entity {}. Skipping cost calculation.",
                entityId);
        }
    }

    /**
     *  Recording the storage costs on the journal for a time unit of a month.
     *  For monthly prices, there are two cases:
     *  1) A flat monthly fee. In this case, there should just be one price in the list with no end range.
     *  2) A list of monthly ranges - e.g. $5 for a 10GB disk, $7 for a 20GB disk, and so on.
     *
     *  In both cases, we just loop through the list until we find the price whose end range is less
     *  than the amount required by the volume, if the endRange is unset or 0 we treat it like infinity.
     *
     * @param monthlyPrices     contains all prices for month unit.
     * @param amountCapacityGb  amount capacity of the volume in gb.
     * @param storageTier       that we are calculating.
     * @param journal           used to add the costs to.
     */
    private void recordRangePricesForMonth(List<Price> monthlyPrices, float amountCapacityGb,
                               CostJournal.Builder<ENTITY_CLASS> journal, ENTITY_CLASS storageTier) {
        if (!CollectionUtils.isEmpty(monthlyPrices) &&
                // 0 capacity shouldn't get charged anything.
            amountCapacityGb > 0) {
            Price price = null;
            for (final Price rangePrice : monthlyPrices) {
                price = rangePrice;
                final long endRange = rangePrice.getEndRangeInUnits() > 0
                    ? rangePrice.getEndRangeInUnits() : Long.MAX_VALUE;
                if (amountCapacityGb < endRange) {
                    break;
                }
            }
            journal.recordOnDemandCost(CostCategory.STORAGE,
                storageTier,
                // This won't be null because we check if collection is null/empty.
                Objects.requireNonNull(price),
                // No RI, so we are buying "100%" of the storage for on-demand prices.
                1.0);
        }
    }

    /**
     * Helper function to sort the prices by units for future usage.
     *
     * @param  storageTierPrices contains prices to sort.
     * @return mapping between price unit to a list of prices for this unit.
     */
    private Map<Price.Unit, List<Price>> createSortedStoragePriceMap(final StorageTierPriceList storageTierPrices) {
        Map<Price.Unit, List<Price>> pricesByUnit =
            storageTierPrices.getCloudStoragePriceList().stream()
                .flatMap(storageTierPrice -> storageTierPrice.getPricesList().stream())
                .collect(Collectors.groupingBy(Price::getUnit));
        // Sort each price list by end range.
        pricesByUnit.values().forEach(priceList -> priceList.sort((price1, price2) -> {
            final long endRange1 = price1.getEndRangeInUnits() > 0 ? price1.getEndRangeInUnits() : Long.MAX_VALUE;
            final long endRange2 = price2.getEndRangeInUnits() > 0 ? price2.getEndRangeInUnits() : Long.MAX_VALUE;
            return Long.compare(endRange1, endRange2);
        }));
        return pricesByUnit;
    }

    /**
     * Recording the storage costs into the journal.
     *
     * @param pricesByUnit  Map from price unit to a list of prices.
     * @param journal       Journal used to add the costs to.
     * @param storageTier   Storage Tier that will be recorded.
     * @param amountToBuy   How much to buy.
     * @param priceUnit     Price unit in hours/days/months and so on.
     */
    private void recordStorageRangePricesByUnit(Map<Price.Unit, List<Price>> pricesByUnit,
                        CostJournal.Builder<ENTITY_CLASS> journal, ENTITY_CLASS storageTier,
                        double amountToBuy, Unit priceUnit) {
        List<Price> prices = pricesByUnit.get(priceUnit);
        if (!CollectionUtils.isEmpty(prices)) {
            logger.trace("Recording {} costs from prices: {}", priceUnit.name(),
                pricesByUnit.get(priceUnit));
            recordPriceRangeEntries(amountToBuy,
                pricesByUnit.getOrDefault(priceUnit, Collections.emptyList()),
                (price, amount) -> journal.recordOnDemandCost(CostCategory.STORAGE,
                    storageTier,
                    price,
                    amount));
        }
    }

    /**
     *  Recording vm volumes costs into the journal.
     *  For storage costs, the primary entity for cost calculation is the volume.
     *  The VM's storage cost just inherits the volumes.
     *
     *  Note - the volume may not have been processed yet. We're not looking for its cost
     *  journal at this time. We are simply recording the dependency in the VM's cost journal.
     *  The actual lookup will happen when someone tries to get the hourly cost from the
     *  VM's cost journal, and we do assume that that will happen after all cost calculations have
     *  been completed.
     *
     * @param entity    The entity to calculate cost for.
     * @param region    The region that the entity belongs to.
     * @param journal   The journal that is used to add the costs to.
     */
    private void calculateVirtualMachineCost(
                @Nonnull final ENTITY_CLASS entity,
                @Nonnull final ENTITY_CLASS region,
                @Nonnull final CostJournal.Builder<ENTITY_CLASS> journal) {
        final long entityId = entityInfoExtractor.getId(entity);
        logger.trace("Starting entity cost calculation for vm {}", entityId);
        final ReservedInstanceApplicator<ENTITY_CLASS> reservedInstanceApplicator =
                reservedInstanceApplicatorFactory.newReservedInstanceApplicator(
                    journal, entityInfoExtractor, cloudCostData, topologyRICoverage);
        // Recording vm volumes costs into the journal.
        // For storage costs, the primary entity for cost calculation is the volume.
        // The VM's storage cost just inherits the volumes.
        //
        // Note - the volume may not have been processed yet. We're not looking for its cost
        // journal at this time. We are simply recording the dependency in the VM's cost journal.
        // The actual lookup will happen when someone tries to get the hourly cost from the
        // VM's cost journal, and we do assume that that will happen after all cost calculations have
        // been completed.
        cloudTopology.getConnectedVolumes(entityId).forEach(journal::inheritCost);

        entityInfoExtractor.getComputeConfig(entity).ifPresent(computeConfig -> {
            // Calculate on-demand prices for entities that have a compute config.
            cloudTopology.getComputeTier(entityId).ifPresent(computeTier -> {

                // Apply the reserved instance coverage, and return the percent of the entity's compute that's
                // covered by reserved instances.
                double riComputeCoveragePercent = reservedInstanceApplicator.recordRICoverage(computeTier);
                Preconditions.checkArgument(
                    riComputeCoveragePercent >= 0.0 && riComputeCoveragePercent <= 1.0);
                final long regionId = entityInfoExtractor.getId(region);
                recordVMSpotInstanceCost(regionId, computeTier, riComputeCoveragePercent, journal, computeConfig);
                final OnDemandPriceTable onDemandPriceTable =
                    cloudCostData.getPriceTable().getOnDemandPriceByRegionIdMap().get(regionId);
                if (onDemandPriceTable != null) {
                    if (computeConfig.getBillingType() != VMBillingType.BIDDING) {
                        final ComputeTierPriceList computePriceList =
                            onDemandPriceTable.getComputePricesByTierIdMap()
                                .get(entityInfoExtractor.getId(computeTier));
                        if (computePriceList != null) {
                            final ComputeTierConfigPrice basePrice = computePriceList.getBasePrice();
                            // For compute tiers, we're working with "hourly" costs, and the amount
                            // of "compute" bought from the tier is the percentage of the hour filled
                            // by on-demand coverage (i.e. 1 - % RI coverage).
                            final double unitsBought = 1 - riComputeCoveragePercent;
                            recordOnDemandVmCost(journal, unitsBought, basePrice, computeTier);
                            recordVMLicenseCost(journal, computeTier, computeConfig,
                                computePriceList, unitsBought);
                        }
                    }
                    recordVMIpCost(entity, computeTier, onDemandPriceTable, journal);
                } else {
                    logger.warn("calculateVirtualMachineCost: Global price table has no entry for region {}." +
                            "  This means there is some inconsistency between the topology and pricing data.",
                        regionId);
                }
            });
        });
    }

    /**
     * Calculate vm price and add it to the journal, taking into consideration the licence, OS, and RI.
     *
     * @param journal                  Used to add the costs to.
     * @param computeTier              Compute Tier that we are calculating.
     * @param unitsBought              Amount of units bought.
     * @param computeConfig            Compute configuration of a the computeTier.
     * @param computePriceList         List that contains all the prices.
     */
    private void recordVMLicenseCost(CostJournal.Builder<ENTITY_CLASS> journal, ENTITY_CLASS computeTier,
                                     ComputeConfig computeConfig,
                                     ComputeTierPriceList computePriceList, double unitsBought) {
        if (computeConfig.getLicenseModel() == LicenseModel.LICENSE_INCLUDED) {
            LicensePriceTuple licensePrice = cloudCostData.getLicensePriceForOS(computeConfig.getOs(),
                    computeConfig.getNumCores(), computePriceList);

            // Recording any price adjustments from the license base price.
            if (licensePrice.getImplicitLicensePrice() != LicensePriceTuple.NO_LICENSE_PRICE) {
                journal.recordOnDemandCost(CostCategory.LICENSE, computeTier,
                        Price.newBuilder().setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(licensePrice.getImplicitLicensePrice()).build())
                                .build(), unitsBought);
            }

            // Recording the license price according to os and number of cores (used for explicit cases).
            // Here the "unitsBought" is 1.0 since the RI discount does not apply on explicit price.
            if (licensePrice.getExplicitLicensePrice() != LicensePriceTuple.NO_LICENSE_PRICE) {
                journal.recordOnDemandCost(CostCategory.LICENSE, computeTier,
                        Price.newBuilder().setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(licensePrice.getExplicitLicensePrice()).build())
                                .build(), 1.0);
            }
        } else {
            // The VM is "Bring Your Own License" - set the license price to 0.
            journal.recordOnDemandCost(CostCategory.LICENSE, computeTier,
                    Price.newBuilder().setPriceAmount(CurrencyAmount.newBuilder()
                            .setAmount(0).build()).build(), 0);
        }
    }

    private void recordOnDemandVmCost(CostJournal.Builder<ENTITY_CLASS> journal, double unitsBought,
                                      ComputeTierConfigPrice basePrice, ENTITY_CLASS computeTier) {
        journal.recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, computeTier,
            basePrice.getPricesList().get(0), unitsBought);
    }

    /**
     * Compute IP price and add it to the compute cost journal.
     *
     * @param entity             Entity that we are calculating price for.
     * @param computeTier        Compute Tier that we are calculating.
     * @param onDemandPriceTable PriceTable that contains the prices.
     * @param journal            Journal used to add the costs to.
     */
    private void recordVMIpCost(ENTITY_CLASS entity, ENTITY_CLASS computeTier, OnDemandPriceTable onDemandPriceTable,
                                CostJournal.Builder<ENTITY_CLASS> journal) {
        entityInfoExtractor.getNetworkConfig(entity).ifPresent(networkConfigBought -> {
            Optional<ENTITY_CLASS> service = cloudTopology.getConnectedService(
                    entityInfoExtractor.getId(computeTier));
            // Checks if the connected service of the entity exists.
            if (service.isPresent()) {
                // There can be only 1 entry in the priceList in the current implementation
                onDemandPriceTable.getIpPrices().getIpPriceList().stream()
                        .findFirst()
                        .ifPresent(ipPriceList -> {
                            // Excess of Elastic IPs needed beyond the freeIPs available in the region
                            long numElasticIps = networkConfigBought.getNumElasticIps()
                                    - ipPriceList.getFreeIpCount();
                            recordPriceRangeEntries(numElasticIps,
                                    ipPriceList.getPricesList(),
                                    (price, amountBought) -> journal.recordOnDemandCost(CostCategory.IP,
                                            service.get(), price, amountBought));
                        });
            } else {
                logger.error("Connected service is not available to calculate IP price for" +
                                " {} with ID {} with compute tier {} with ID {}",
                        entityInfoExtractor.getName(entity), entityInfoExtractor.getId(entity),
                        entityInfoExtractor.getName(computeTier),
                        entityInfoExtractor.getId(computeTier));
            }
        });
    }

    /**
     * Record vm prices for spot instance/low priority vms and add it to the compute cost journal.
     *
     * @param regionId                 Region id to get the spot instances in.
     * @param computeTier              Compute Tier that we are calculating.
     * @param riComputeCoveragePercent RI to calculate the coverage.
     * @param journal                  Journal used to add the costs to.
     * @param computeConfig            Compute configuration of the computeTier.
     */
    private void recordVMSpotInstanceCost(long regionId, ENTITY_CLASS computeTier, double riComputeCoveragePercent,
                                          CostJournal.Builder<ENTITY_CLASS> journal, ComputeConfig computeConfig) {
        if (computeConfig.getBillingType() == VMBillingType.BIDDING) {
            final Pricing.SpotInstancePriceTable spotPriceTable =
                cloudCostData.getPriceTable().getSpotPriceByRegionIdMap().get(regionId);
            if (spotPriceTable != null) {
                Price spotPrice = spotPriceTable.getSpotPriceByInstanceIdMap()
                    .get(entityInfoExtractor.getId(computeTier));
                final double unitsBought = 1 - riComputeCoveragePercent;
                journal.recordOnDemandCost(CostCategory.SPOT, computeTier,
                    spotPrice, unitsBought);
            }
        }
    }

    private void calculateDatabaseCost(@Nonnull final ENTITY_CLASS entity,
                                       @Nonnull final ENTITY_CLASS region,
                                       final boolean isDbServer,
                                       @Nonnull final CostJournal.Builder<ENTITY_CLASS> journal) {
        final long entityId = entityInfoExtractor.getId(entity);
        logger.trace("Starting entity cost calculation for db {}", entityId);
        entityInfoExtractor.getDatabaseConfig(entity).ifPresent(databaseConfig -> {
            // Calculate on-demand prices for entities that have a database config.
            final Optional<ENTITY_CLASS> tier;
            if (isDbServer) {
                tier = cloudTopology.getDatabaseServerTier(entityId);
            } else {
                tier = cloudTopology.getDatabaseTier(entityId);
            }
            tier.ifPresent(databaseTier -> {
                final long regionId = entityInfoExtractor.getId(region);
                final OnDemandPriceTable onDemandPriceTable =
                    cloudCostData.getPriceTable().getOnDemandPriceByRegionIdMap().get(regionId);
                if (onDemandPriceTable != null) {
                    final DatabaseTierPriceList dbPriceList =
                            onDemandPriceTable.getDbPricesByInstanceIdMap()
                                    .get(entityInfoExtractor.getId(databaseTier));
                    if (dbPriceList != null) {
                        recordDatabaseCost(dbPriceList, journal, databaseTier, databaseConfig);
                    }
                } else {
                    logger.warn("calculateDatabaseCost: Global price table has no entry for region {}." +
                            "  This means there is some inconsistency between the topology and pricing data.", regionId);
                }
            });
        });
    }

    /**
     * Record db prices and add it to the compute cost journal.
     *
     * @param dbPriceList     DB list contains all the db prices.
     * @param journal         Journal used to add the costs to.
     * @param databaseTier    DB Tier that we are calculating.
     * @param databaseConfig  DB config of the db that we want to record.
     */
    private void recordDatabaseCost(DatabaseTierPriceList dbPriceList, CostJournal.Builder<ENTITY_CLASS> journal,
                                    ENTITY_CLASS databaseTier, DatabaseConfig databaseConfig){
        final DatabaseTierConfigPrice basePrice = dbPriceList.getBasePrice();
        final double amountBought = 1;
        journal.recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, databaseTier,
            basePrice.getPricesList().get(0), amountBought);
        dbPriceList.getConfigurationPriceAdjustmentsList().stream()
            .filter(databaseConfig::matchesPriceTableConfig)
            .findAny()
            .ifPresent(priceAdjustmentConfig -> journal.recordOnDemandCost(
                CostCategory.LICENSE,
                databaseTier,
                priceAdjustmentConfig.getPricesList().get(0), amountBought));
    }

    /**
     * Given an amount to buy and a list of "tiered" prices, record the amount bought at each
     * price. For example, if we're buying 7 units with two prices:
     *    1. $5/unit with end range of 5
     *    2. $3/unit with end range infinity
     *
     * This method will call back to the recordFn with:
     *    - Price 1, amount: 5
     *    - Price 2, amount: 2
     *
     * @param amountToBuy The total amount bought.
     * @param prices      The tiered list of prices. Prices should be arranged in increasing order
     *                    by endRangeInUnits.
     * @param recordFn    Callback to record prices and amounts bought at those prices.
     */
    private void recordPriceRangeEntries(final double amountToBuy,
                                         @Nonnull final List<Price> prices,
                                         @Nonnull final BiConsumer<Price, Double> recordFn) {
        if (amountToBuy <= 0) {
            return;
        }
        double remainingToBuy = amountToBuy;
        long lastPriceRangeEnd = 0;
        for (Price price : prices) {
            // Either buy the entire remaining capacity, or
            // the entire amount available at this range.
            if (price.getEndRangeInUnits() < 0) {
                logger.warn("Illegal negative end range {} for price {}." +
                    "Skipping this price for calculation.", price.getEndRangeInUnits(), price);
            } else {
                // 0 indicates the default/unset value.
                final long curEndRange = price.getEndRangeInUnits() == 0 ?
                        Long.MAX_VALUE : price.getEndRangeInUnits();
                final double amountBought = Math.min(remainingToBuy,
                        curEndRange - lastPriceRangeEnd);
                remainingToBuy -= amountBought;
                lastPriceRangeEnd = price.getEndRangeInUnits();
                if (amountBought > 0) {
                    recordFn.accept(price, amountBought);
                    if (remainingToBuy <= 0) {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        if (remainingToBuy > 0) {
            // If this happens, it means the price range is limited and we are trying to "buy"
            // more than is possible. This shouldn't really happen, because in the cloud you
            // can always buy more.
            logger.error("Bad price list could not accommodate a purchase of {} units. Prices: {}",
                prices);
        }
    }

    /**
     * Create a new production {@link CloudCostCalculatorFactory}.
     *
     * @param <ENTITY_CLASS> The class of entities used in the calculators produced by the factory.
     * @return The {@link CloudCostCalculatorFactory}.
     */
    public static <ENTITY_CLASS> CloudCostCalculatorFactory<ENTITY_CLASS> newFactory() {
        return new CloudCostCalculatorFactory<ENTITY_CLASS>() {
            @Nonnull
            @Override
            public CloudCostCalculator<ENTITY_CLASS> newCalculator(
                    @Nonnull final CloudCostData cloudCostData,
                    @Nonnull final CloudTopology<ENTITY_CLASS> cloudTopology,
                    @Nonnull final EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor,
                    @Nonnull final DiscountApplicatorFactory<ENTITY_CLASS> discountApplicatorFactory,
                    @Nonnull final ReservedInstanceApplicatorFactory<ENTITY_CLASS> riApplicatorFactory,
                    @Nonnull final DependentCostLookup<ENTITY_CLASS> dependentCostLookup,
                    @Nonnull final Map<Long, EntityReservedInstanceCoverage> topologyRICoverage) {
                return new CloudCostCalculator<>(cloudCostData, cloudTopology, entityInfoExtractor,
                    discountApplicatorFactory, riApplicatorFactory,
                    dependentCostLookup, topologyRICoverage);
            }
        };
    }

    /**
     * A factory for {@link CloudCostCalculator} instances. Mainly for unit-testing purposes.
     *
     * @param <ENTITY_CLASS> The class of entities used in this calculator.
     */
    @FunctionalInterface
    public interface CloudCostCalculatorFactory<ENTITY_CLASS> {

        /**
         * Create a new {@link CloudCostCalculator}.
         *
         * @param cloudCostData Cost information.
         * @param cloudTopology The cloud topology to use for cost calculation.
         * @param entityInfoExtractor Extracts properties from entities in the topology.
         * @param discountApplicatorFactory Applies discounts.
         * @param riApplicatorFactory Figures out per-entity RI coverage precentage.
         * @param dependentCostLookup Lookup for cost dependencies (e.g. VM -> Volume).
         * @param topologyRICoverage RI coverage information.
         * @return The cost calculator.
         */
        @Nonnull
        CloudCostCalculator<ENTITY_CLASS> newCalculator(
                @Nonnull CloudCostData cloudCostData,
                @Nonnull CloudTopology<ENTITY_CLASS> cloudTopology,
                @Nonnull EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor,
                @Nonnull DiscountApplicatorFactory<ENTITY_CLASS> discountApplicatorFactory,
                @Nonnull ReservedInstanceApplicatorFactory<ENTITY_CLASS> riApplicatorFactory,
                @Nonnull DependentCostLookup<ENTITY_CLASS> dependentCostLookup,
                @Nonnull Map<Long, EntityReservedInstanceCoverage> topologyRICoverage);
    }
}
