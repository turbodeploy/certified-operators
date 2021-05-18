package com.vmturbo.cost.calculation;

import static com.vmturbo.commons.Units.GBYTE;
import static com.vmturbo.commons.Units.MBYTE;
import static com.vmturbo.trax.Trax.trax;
import static com.vmturbo.trax.Trax.traxConstant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Pricing.DbServerTierOnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.DbTierOnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable.PriceForGuestOsType;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable.SpotPricesForTier;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.utils.FuzzyDouble;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.LicensePriceTuple;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeTierConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.DatabaseConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.VirtualVolumeConfig;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.journal.CostJournal.Builder;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LicenseModel;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseServerTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseServerTierPriceList.DatabaseServerTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList.StorageTierPrice;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxConfiguration.TraxContext;
import com.vmturbo.trax.TraxNumber;

/**
 * This is the main entry point into the cost calculation library. The user is responsible for
 * providing implementations of the integration classes to the constructor.
 *
 * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
 *                      {@link TopologyEntityDTO} for the realtime topology.
 */
public class CloudCostCalculator<ENTITY_CLASS> {

    private static final Logger logger = LogManager.getLogger();

    private static final TraxNumber FULL = traxConstant(1.0, "100%");

    private static final Set<Integer> ENTITY_TYPES_WITH_COST = ImmutableSet.of(
                                        EntityType.VIRTUAL_MACHINE_VALUE,
                                        EntityType.DATABASE_SERVER_VALUE,
                                        EntityType.DATABASE_VALUE,
                                        EntityType.VIRTUAL_VOLUME_VALUE);

    private final CloudCostData<ENTITY_CLASS> cloudCostData;

    private final CloudTopology<ENTITY_CLASS> cloudTopology;

    private final EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor;

    private final ReservedInstanceApplicatorFactory<ENTITY_CLASS> reservedInstanceApplicatorFactory;

    private final DependentCostLookup<ENTITY_CLASS> dependentCostLookup;

    private final Map<Long, EntityReservedInstanceCoverage> topologyRICoverage;

    private CloudCostCalculator(@Nonnull final CloudCostData<ENTITY_CLASS> cloudCostData,
               @Nonnull final CloudTopology<ENTITY_CLASS> cloudTopology,
               @Nonnull final EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor,
               @Nonnull final ReservedInstanceApplicatorFactory<ENTITY_CLASS> reservedInstanceApplicatorFactory,
               @Nonnull final DependentCostLookup<ENTITY_CLASS> dependentCostLookup,
               @Nonnull final Map<Long, EntityReservedInstanceCoverage> topologyRICoverage) {
        this.cloudCostData = Objects.requireNonNull(cloudCostData);
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.entityInfoExtractor = Objects.requireNonNull(entityInfoExtractor);
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
        return calculateCost(entity, false);
    }

    /**
     * Calculate the cost for a single entity. The cost includes VMBillingType.BIDDING if isProjectedTopology is set to true.
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
     * @param isProjectedTopology Is this cost calculation for an entity in a projected topology?
     * @return A {@link CostJournal} with the cost breakdown. Use methods on the cost journal
     *         to get the actual costs.
     */
    @Nonnull
    public CostJournal<ENTITY_CLASS> calculateCost(@Nonnull final ENTITY_CLASS entity, final boolean isProjectedTopology) {
        final long entityId = entityInfoExtractor.getId(entity);
        final int entityType = entityInfoExtractor.getEntityType(entity);
        if (!ENTITY_TYPES_WITH_COST.contains(entityType)) {
            logger.debug("Skipping cost calculation for entity {} due to unsupported entity type {}",
                entityId, EntityType.forNumber(entityType).name());
            return CostJournal.empty(entity, entityInfoExtractor);
        }

        final String entityTypeName = ApiEntityType.fromSdkTypeToEntityTypeString(entityType);
        final String entityName = entityInfoExtractor.getName(entity);
        try (TraxContext traxContext = Trax.track(entityTypeName, entityName, Long.toString(entityId), "COST")) {
            final Optional<ENTITY_CLASS> regionOpt = cloudTopology.getConnectedRegion(entityId);
            final Optional<ENTITY_CLASS> businessAccountOpt = cloudTopology.getOwner(entityId);

            if (!businessAccountOpt.isPresent()) {
                logger.warn("Unable to find business account for entity {}. Returning empty cost.", entityId);
                return CostJournal.empty(entity, entityInfoExtractor);
            }

            final ENTITY_CLASS businessAccount = businessAccountOpt.get();

            final long businessAccountOid = entityInfoExtractor.getId(businessAccount);
            Optional<AccountPricingData<ENTITY_CLASS>> accountPricingDataOpt =
                    cloudCostData.getAccountPricingData(businessAccountOid);
            if (!accountPricingDataOpt.isPresent()) {
                return CostJournal.empty(entity, entityInfoExtractor);
            }
            AccountPricingData<ENTITY_CLASS> accountPricingData = accountPricingDataOpt.get();
            if (!regionOpt.isPresent()) {
                logger.warn("Unable to find region for entity {}. Returning empty cost.", entityId);
                return CostJournal.empty(entity, entityInfoExtractor);
            }
            final ENTITY_CLASS region = regionOpt.get();
            final DiscountApplicator<ENTITY_CLASS> discountApplicator = accountPricingData
                    .getDiscountApplicator();

            final CostJournal.Builder<ENTITY_CLASS> journal =
                CostJournal.newBuilder(entity, entityInfoExtractor, region, discountApplicator, dependentCostLookup);

            long regionId = entityInfoExtractor.getId(region);

            final PriceTable priceTable = accountPricingData.getPriceTable();
            final Optional<OnDemandPriceTable> onDemandPriceTable =
                    Optional.ofNullable(priceTable.getOnDemandPriceByRegionIdMap().get(regionId));

            // Get Spot price table by Availability Zone (if present) or by Region (if Availability
            // Zone is missing)
            final Map<Long, SpotInstancePriceTable> spotInstancePriceTableMap = priceTable
                    .getSpotPriceByZoneOrRegionIdMap();
            final SpotInstancePriceTable spotInstancePriceTable = cloudTopology
                    .getConnectedAvailabilityZone(entityId)
                    .map(entityInfoExtractor::getId)
                    .filter(spotInstancePriceTableMap::containsKey)
                    .map(spotInstancePriceTableMap::get)
                    .orElse(spotInstancePriceTableMap.get(regionId));
            final CostCalculationContext<ENTITY_CLASS> context = new CostCalculationContext<>(
                    journal, entity, regionId, accountPricingData, onDemandPriceTable,
                    Optional.ofNullable(spotInstancePriceTable));

            switch (entityInfoExtractor.getEntityType(entity)) {
                case EntityType.VIRTUAL_MACHINE_VALUE:
                    calculateVirtualMachineCost(context, isProjectedTopology);
                    break;
                case EntityType.DATABASE_VALUE:
                    calculateDatabaseCost(context);
                    break;
                case EntityType.DATABASE_SERVER_VALUE:
                    calculateDatabaseServerCost(context);
                    break;
                case EntityType.VIRTUAL_VOLUME_VALUE:
                    calculateVirtualVolumeCost(context);
                    break;
                default:
                    logger.error("Received invalid entity " + entity.toString());
                    break;
            }
            final CostJournal<ENTITY_CLASS> builtJournal = journal.build();
            if (traxContext.on()) {
                logger.debug("Cost calculation stack for {} \"{}\" {}:\n{}",
                    () -> entityTypeName, () -> entityName, () -> entityId,
                    () -> builtJournal.getTotalHourlyCost().calculationStack());
            }
            return builtJournal;
        }
    }

    private void calculateVirtualVolumeCost(@Nonnull CostCalculationContext<ENTITY_CLASS> context) {
        final ENTITY_CLASS entity = context.getEntity();
        final CostJournal.Builder<ENTITY_CLASS> journal = context.getCostJournal();
        final long entityId = entityInfoExtractor.getId(entity);
        logger.trace("Starting entity cost calculation for volume {}", entityId);
        final Optional<VirtualVolumeConfig> volumeConfigOpt = entityInfoExtractor.getVolumeConfig(entity);
        if (volumeConfigOpt.isPresent()) {
            final VirtualVolumeConfig volumeConfig = volumeConfigOpt.get();
            // Ephemeral volumes are directly attached to the EC2 instance, so there's no need to calculate the cost.
            if (volumeConfig.isEphemeral()) {
                logger.debug("Skipping Volume cost calculation, {} is Ephemeral", entityId);
                return;
            }
            final Optional<ENTITY_CLASS> storageTierOpt = cloudTopology.getStorageTier(entityId);
            if (storageTierOpt.isPresent()) {
                final ENTITY_CLASS storageTier = storageTierOpt.get();
                final long regionId = context.getRegionid();
                final long storageTierId = entityInfoExtractor.getId(storageTier);
                final OnDemandPriceTable onDemandPriceTable = context.getOnDemandPriceTable().isPresent() ?
                        context.getOnDemandPriceTable().get() : null;
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
                        final RedundancyType redundancyType = volumeConfig.getRedundancyType();
                        final Map<Price.Unit, List<Price>> pricesByUnit =
                            createSortedStoragePriceMap(storageTierPrices, redundancyType);
                        final String redundancyTypeSuffix = redundancyType != null ?
                            "(Redundancy type: " + redundancyType + ")" : "";
                        recordStorageRangePricesByUnit(pricesByUnit, journal, storageTier,
                            trax(volumeConfig.getAccessCapacityMillionIops(), "access capacity " +
                                "million iops " + redundancyTypeSuffix), Unit.MILLION_IOPS);
                        recordStorageRangePricesByUnit(pricesByUnit, journal, storageTier,
                            trax(volumeConfig.getIoThroughputCapacityMBps(), "throughput capacity in MiB/s"),
                            Unit.MBPS_MONTH);
                        recordStorageRangePricesByUnit(pricesByUnit, journal, storageTier,
                            trax(volumeConfig.getAmountCapacityGb(), "capacity gb/month "
                                + redundancyTypeSuffix), Unit.GB_MONTH);
                        final TraxNumber hourlyBilledOps = trax(volumeConfig.getHourlyBilledOps(),
                                "billed I/O operations per hour");
                        recordStorageRangePricesByUnit(pricesByUnit, journal, storageTier,
                                hourlyBilledOps, Unit.IO_REQUESTS);
                        recordRangePricesForMonth(pricesByUnit.get(Unit.MONTH),
                            volumeConfig.getAmountCapacityGb(), journal, storageTier);
                    } else {
                        logger.error("Could not calculate cost for Virtual volume {}. Price table " +
                                "for region {} has no entry for tier {}. Skipping cost " +
                                "calculation.", entityId, regionId, storageTierId);
                    }
                } else {
                    logger.debug("calculateVirtualVolumeCost: Global price table has no entry for region {}." +
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
     *  In both cases, we just loop through the list until we find the price whose end range is
     *  equal or less than the amount required by the volume, if the endRange is unset or 0 we
     *  treat it like infinity.
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
                if (amountCapacityGb <= endRange) {
                    break;
                }
            }
            journal.recordOnDemandCost(CostCategory.STORAGE,
                storageTier,
                // This won't be null because we check if collection is null/empty.
                Objects.requireNonNull(price),
                // No RI, so we are buying "100%" of the storage for on-demand prices.
                FULL);
        }
    }

    /**
     * Helper function to sort the prices by units for future usage.
     *
     * @param  storageTierPrices contains prices to sort.
     * @param volumeRedundancyType redundancy type of the volume for which the storage price map
     *                             is being created, null if no redundancy type applicable
     * @return mapping between price unit to a list of prices for this unit.
     */
    private Map<Price.Unit, List<Price>> createSortedStoragePriceMap(final StorageTierPriceList storageTierPrices,
                                                                     @Nullable final RedundancyType
                                                                         volumeRedundancyType) {
        final Map<Price.Unit, List<Price>> pricesByUnit =
            storageTierPrices.getCloudStoragePriceList().stream()
                .filter(priceList -> redundancyTypeNotApplicable(priceList)
                        || defaultRedundancyTypeApplicable(volumeRedundancyType, priceList)
                        || redundancyTypesMatch(volumeRedundancyType, priceList))
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
     * The redundancy type is not applicable if price entry does not have a redundancy type. The
     * price will be included even if volume has a redundancy type. It can happen when migration
     * a volume from Azure to AWS.
     *
     * @param storageTierPrice storage tier price
     * @return true is storage tier price does not have a redundancy type; false otherwise.
     */
    private static boolean redundancyTypeNotApplicable(final StorageTierPrice storageTierPrice) {
        return !storageTierPrice.hasRedundancyType();
    }

    /**
     * Use the default redundancy type when migrating a volume without redundancy type to a tier
     * that supports redundancy type.
     * For example, AWS and on-prem volumes don't have a redundancy type. When migrating a volume
     * to Azure, assume redundancy type is LRS.
     *
     * @param volumeRedundancyType volume redundancy type
     * @param storageTierPrice storage tier price
     * @return true if volume redundancy type is null and price has LRS redundancy type; false otherwise.
     */
    private static boolean defaultRedundancyTypeApplicable(final RedundancyType volumeRedundancyType,
                                                           final StorageTierPrice storageTierPrice) {
        return volumeRedundancyType == null && storageTierPrice.hasRedundancyType()
                && storageTierPrice.getRedundancyType() == RedundancyType.LRS;
    }

    private static boolean redundancyTypesMatch(final RedundancyType volumeRedundancyType,
                                                final StorageTierPrice storageTierPrice) {
        return volumeRedundancyType != null && storageTierPrice.hasRedundancyType()
            && volumeRedundancyType == storageTierPrice.getRedundancyType();
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
                        TraxNumber amountToBuy, Unit priceUnit) {
        List<Price> prices = pricesByUnit.get(priceUnit);
        if (!CollectionUtils.isEmpty(prices)) {
            logger.trace("Recording {} costs from prices: {}", priceUnit.name(), prices);
            recordPriceRangeEntries(amountToBuy, prices,
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
     * @param context The context containing info about the entity, region, business account, journal,
     *                and onDemandPriceTable
     */
    private void calculateVirtualMachineCost(
                @Nonnull CostCalculationContext<ENTITY_CLASS> context, boolean isProjectedTopology) {
        final ENTITY_CLASS entity = context.getEntity();
        final long entityId = entityInfoExtractor.getId(entity);
        final CostJournal.Builder<ENTITY_CLASS> journal = context.getCostJournal();
        final AccountPricingData accountPricingData = context.getAccountPricingData();
        logger.trace("Starting entity cost calculation for vm {}", entityId);
        final ReservedInstanceApplicator<ENTITY_CLASS> reservedInstanceApplicator =
                reservedInstanceApplicatorFactory.newReservedInstanceApplicator(
                    context.getCostJournal(), entityInfoExtractor, cloudCostData, topologyRICoverage);
        // Recording vm volumes costs into the journal.
        // For storage costs, the primary entity for cost calculation is the volume.
        // The VM's storage cost just inherits the volumes.
        //
        // Note - the volume may not have been processed yet. We're not looking for its cost
        // journal at this time. We are simply recording the dependency in the VM's cost journal.
        // The actual lookup will happen when someone tries to get the hourly cost from the
        // VM's cost journal, and we do assume that that will happen after all cost calculations have
        // been completed.
        cloudTopology.getAttachedVolumes(entityId).forEach(journal::inheritCost);

        entityInfoExtractor.getComputeConfig(entity).ifPresent(computeConfig -> {
            // Calculate on-demand prices for entities that have a compute config.
            cloudTopology.getComputeTier(entityId).ifPresent(computeTier -> {
                final long regionId = context.getRegionid();
                final Optional<OnDemandPriceTable> onDemandPriceTable = context.getOnDemandPriceTable();
                if (!onDemandPriceTable.isPresent()) {
                    logger.debug("calculateVirtualMachineCost: Global price table has no entry for region {}." +
                                    "  This means there is some inconsistency between the topology and pricing data.",
                            regionId);
                } else {
                    final ComputeTierPriceList computePriceList = onDemandPriceTable.get()
                            .getComputePricesByTierIdMap()
                            .get(entityInfoExtractor.getId(computeTier));
                    LicensePriceTuple licensePrice = null;
                    final Optional<ComputeTierConfig> computeTierConfig = entityInfoExtractor.getComputeTierConfig(computeTier);
                    if (computePriceList != null
                            && (isProjectedTopology || computeConfig.getBillingType() != VMBillingType.BIDDING)
                            // The compute tier should always be present, if we made is this far, given the
                            // cloudTopology.getComputeTier() call above has already checked that the computeTier
                            // has the appropriate entity type
                            && computeTierConfig.isPresent()) {

                        licensePrice = accountPricingData.getLicensePrice(
                                computeTierConfig.get(), computeConfig.getOs(), computePriceList);
                        final ComputeTierConfigPrice basePrice = computePriceList.getBasePrice();
                        // For compute tiers, we're working with "hourly" costs, and the amount
                        // of "compute" bought from the tier is 1 unit, if the VM is powered on. If
                        // powered off, the bought amount is 0. Note: This cost is purely
                        // on demand and does not include any RI related costs.
                        final TraxNumber computeBillableAmount = trax(isBillable(entity) ? 1.0 : 0.0, "On-demand billed amount");
                        recordOnDemandVmCost(journal, computeBillableAmount, basePrice, computeTier);
                        recordOnDemandVMLicenseCost(journal, computeTier, computeConfig, computeBillableAmount, licensePrice);
                    } else if (computeConfig.getBillingType() == VMBillingType.BIDDING) {
                        recordVMSpotInstanceCost(computeTier, journal, context);
                    }
                    Price price = Price.newBuilder().setPriceAmount(CurrencyAmount.newBuilder()
                        .setAmount(licensePrice != null ? licensePrice.getReservedInstanceLicensePrice() : 0).build())
                            .build();
                    // Apply the reserved instance coverage, and return the percent of the entity's compute that's
                    // covered by reserved instances.
                    boolean recordLicenseCost = computeConfig.getLicenseModel() == LicenseModel.LICENSE_INCLUDED;
                    reservedInstanceApplicator.recordRICoverage(computeTier, price, recordLicenseCost);
                    recordVMIpCost(entity, computeTier, onDemandPriceTable.get(), journal);
                }
                // Add entity uptime discounts to all categories
                final FuzzyDouble entityUptimePercentage = FuzzyDouble.newFuzzy(cloudCostData.getEntityUptimePercentage(entityId));
                if (entityUptimePercentage.isLessThan(100.0)) {
                    TraxNumber entityUptimeDiscountValue = trax((entityUptimePercentage.value()),
                            "EntityUptimeDiscountMultiplier");
                    entityUptimeDiscountValue = entityUptimeDiscountValue.times(-1).compute()
                            .plus(100F).compute()
                            .dividedBy(100f).compute();
                    journal.addUptimeDiscountToAllCategories(entityUptimeDiscountValue);
                } else {
                    logger.trace("Skipping entity uptime discount for {}", entityId);
                }

            });
        });
    }

    /**
     * Calculate vm price and add it to the journal, taking into consideration the licence, OS, and RI.
     *  @param journal                  Used to add the costs to.
     * @param computeTier              Compute Tier that we are calculating.
     * @param computeConfig            Compute configuration of a the computeTier.
     * @param unitsBought              Amount of units bought.
     * @param licensePrice             The license price tuple.
     */
    private void recordOnDemandVMLicenseCost(Builder<ENTITY_CLASS> journal, ENTITY_CLASS computeTier,
                                             ComputeConfig computeConfig, TraxNumber unitsBought,
                                             LicensePriceTuple licensePrice) {
        // The units bought for both implicit and explicit license prices will be 1.
        if (computeConfig.getLicenseModel() == LicenseModel.LICENSE_INCLUDED) {
            if (licensePrice.getImplicitOnDemandLicensePrice() != LicensePriceTuple.NO_LICENSE_PRICE) {
                journal.recordOnDemandCost(CostCategory.ON_DEMAND_LICENSE, computeTier,
                        Price.newBuilder().setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(licensePrice.getImplicitOnDemandLicensePrice()).build())
                                .build(), unitsBought);
            }

            // Recording the license price according to os and number of cores (used for explicit cases).
            if (licensePrice.getExplicitOnDemandLicensePrice() != LicensePriceTuple.NO_LICENSE_PRICE) {
                journal.recordOnDemandCost(CostCategory.ON_DEMAND_LICENSE, computeTier,
                        Price.newBuilder().setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(licensePrice.getExplicitOnDemandLicensePrice()).build())
                                .build(), trax(unitsBought.getValue(), "explicit license"));
            }
        } else {
            // The VM is "Bring Your Own License" - set the license price to 0.
            journal.recordOnDemandCost(CostCategory.ON_DEMAND_LICENSE, computeTier,
                    Price.newBuilder().setPriceAmount(CurrencyAmount.newBuilder()
                            .setAmount(0).build()).build(), trax(0, "bring-your-own-license price"));
        }

    }

    private void recordOnDemandVmCost(CostJournal.Builder<ENTITY_CLASS> journal, TraxNumber unitsBought,
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
        EntityState state = entityInfoExtractor.getEntityState(entity);
        entityInfoExtractor.getNetworkConfig(entity).ifPresent(networkConfigBought -> {
            Optional<ENTITY_CLASS> service = cloudTopology.getConnectedService(
                entityInfoExtractor.getId(computeTier));
            // Checks if the connected service of the entity exists.
            if (service.isPresent()) {
                // There can be only 1 entry in the priceList in the current implementation
                onDemandPriceTable.getIpPrices().getIpPriceList().stream()
                    .findFirst()
                    .ifPresent(ipPriceList -> {
                        TraxNumber freeIps = trax(ipPriceList.getFreeIpCount(), "free ips");

                        // If the entity is not in powered on state, it is going to be charged
                        // for even free ips
                        if (state != EntityState.POWERED_ON) {
                            freeIps = freeIps.minus(ipPriceList.getFreeIpCount(),
                                "non-eligible free ips")
                                .compute("free  ips");
                        }

                        // Excess of Elastic IPs needed beyond the freeIPs available in the region
                        final TraxNumber numElasticIps = trax(networkConfigBought.getNumElasticIps(), "elastic ips")
                            .minus(freeIps)
                            .compute("ips to buy");


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
     * @param computeTier              Compute Tier that we are calculating.
     * @param journal                  Journal used to add the costs to.
     * @param context                  The account pricing calculation context
     */
    private void recordVMSpotInstanceCost(ENTITY_CLASS computeTier,
                                          CostJournal.Builder<ENTITY_CLASS> journal,
                                          CostCalculationContext<ENTITY_CLASS> context) {
        final Optional<SpotInstancePriceTable> spotPriceTable = context.getSpotInstancePriceTable();
        if (!spotPriceTable.isPresent()) {
            return;
        }
        final long computeTierOid = entityInfoExtractor.getId(computeTier);
        final SpotPricesForTier spotPricesForTier = spotPriceTable.get()
                .getSpotPricesByTierOidMap().get(computeTierOid);
        if (spotPricesForTier == null) {
            logger.error("Cannot find Spot prices for zone/region {}, compute tier {}",
                    context.getRegionid(), computeTierOid);
            return;
        }
        final ENTITY_CLASS entity = context.getEntity();
        final OSType osType = entityInfoExtractor.getComputeConfig(entity)
                .map(ComputeConfig::getOs)
                .orElse(OSType.UNKNOWN_OS);
        final Optional<Price> spotPrice = spotPricesForTier.getPriceForGuestOsTypeList()
                .stream()
                .filter(priceForGuestOs -> priceForGuestOs.getGuestOsType() == osType)
                .map(PriceForGuestOsType::getPrice)
                .findAny();
        if (!spotPrice.isPresent()) {
            logger.error("Cannot find Spot price for zone/region {}, compute tier {}, OS {}",
                    context.getRegionid(), computeTierOid, osType);
            return;
        }
        final TraxNumber unitsBought = trax(1, "units bought at spot price");
        journal.recordOnDemandCost(CostCategory.SPOT, computeTier, spotPrice.get(), unitsBought);
    }

    private void calculateDatabaseCost(CostCalculationContext<ENTITY_CLASS> context) {
        final ENTITY_CLASS entity = context.getEntity();
        final long entityId = entityInfoExtractor.getId(entity);
        logger.trace("Starting entity cost calculation for db {} (billable={})", entityId, isBillable(entity));

        entityInfoExtractor.getDatabaseConfig(entity).ifPresent(databaseConfig -> {
            // Calculate on-demand prices for entities that have a database config.
            final Optional<ENTITY_CLASS> tier = cloudTopology.getDatabaseTier(entityId);
            tier.ifPresent(databaseTier -> {
                final long regionId = context.getRegionid();
                final Optional<OnDemandPriceTable> onDemandPriceTable = context.getOnDemandPriceTable();
                if (onDemandPriceTable.isPresent()) {
                    final DatabaseTierPriceList dbPriceList = getDbPriceList(databaseConfig,
                        onDemandPriceTable.get(), entityInfoExtractor.getId(databaseTier));
                    if (dbPriceList != null) {
                        recordDatabaseCost(dbPriceList, context.getCostJournal(), databaseTier, databaseConfig, entity);
                    }
                } else {
                    logger.debug("calculateDatabaseCost: Global price table has no entry for region {}." +
                            "  This means there is some inconsistency between the topology and pricing data.", regionId);
                }
            });
        });
    }

    private void calculateDatabaseServerCost(CostCalculationContext<ENTITY_CLASS> context) {
        final ENTITY_CLASS entity = context.getEntity();
        final long entityId = entityInfoExtractor.getId(entity);

        logger.trace("Starting entity cost calculation for dbs {} (isBillable={})", entityId, isBillable(entity));

        entityInfoExtractor.getDatabaseConfig(entity).ifPresent(databaseConfig -> {
            // Calculate on-demand prices for entities that have a database config.
            final Optional<ENTITY_CLASS> tier  = cloudTopology.getDatabaseServerTier(entityId);

            tier.ifPresent(databaseTier -> {
                final long regionId = context.getRegionid();
                final Optional<OnDemandPriceTable> onDemandPriceTable = context.getOnDemandPriceTable();
                if (onDemandPriceTable.isPresent()) {
                    final DatabaseServerTierPriceList dbsPriceList = getDbsPriceList(
                            onDemandPriceTable.get(), entityInfoExtractor.getId(databaseTier));
                    if (dbsPriceList != null) {
                        recordDatabaseServerCost(dbsPriceList, context.getCostJournal(), databaseTier, databaseConfig, entity);
                    } else {
                        logger.debug("calculateDatabaseServerCost: Price table is missed for region {}, tier {} ",
                                regionId, databaseTier);
                    }
                } else {
                    logger.warn("calculateDatabaseServerCost: Global price table has no entry for region {}." +
                            "  This means there is some inconsistency between the topology and pricing data.", regionId);
                }
            });
        });
    }

    @Nullable
    private DatabaseServerTierPriceList getDbsPriceList(OnDemandPriceTable onDemandPriceTable,
            long tierId) {
        DbServerTierOnDemandPriceTable priceTable =
                onDemandPriceTable.getDbsPricesByInstanceIdMap().get(tierId);
        if (priceTable == null) {
            logger.warn("No database server price table found for db tier id {}. Returning null db price list",
                    tierId);
            return null;
        }
        return priceTable.getDbsPricesByTierIdMap().values().iterator().next();
    }

    @Nullable
    private DatabaseTierPriceList getDbPriceList(DatabaseConfig databaseConfig,
                                                 OnDemandPriceTable onDemandPriceTable,
                                                 long tierId) {
        DbTierOnDemandPriceTable priceTable =
            onDemandPriceTable.getDbPricesByInstanceIdMap().get(tierId);
        if (priceTable == null) {
            logger.warn("No database price table found for db tier id {}. Returning null db price list",
                    tierId);
            return null;
        }
        Optional<CloudCostDTO.DeploymentType> deploymentType =
            databaseConfig.getDeploymentType();

        if (deploymentType.isPresent()) {
            return priceTable.getDbPricesByDeploymentTypeOrDefault(deploymentType.get().getNumber(),
                null);
        } else {
            return priceTable.hasOnDemandPricesNoDeploymentType() ?
                priceTable.getOnDemandPricesNoDeploymentType() : null;
        }
    }

    /**
     * Record db prices and add it to the compute cost journal.
     *
     * @param dbPriceList    DB list contains all the db prices.
     * @param journal        Journal used to add the costs to.
     * @param databaseTier   DB Tier that we are calculating.
     * @param databaseConfig DB config of the db that we want to record.
     * @param entity         current DB entity.
     */
    private void recordDatabaseCost(DatabaseTierPriceList dbPriceList, Builder<ENTITY_CLASS> journal,
                                    ENTITY_CLASS databaseTier, DatabaseConfig databaseConfig,
                                    final ENTITY_CLASS entity) {
        final DatabaseTierConfigPrice basePrice = dbPriceList.getBasePrice();
        List<Price> storagePrices = dbPriceList.getDependentPricesList();

        final TraxNumber computeBillableAmount = trax(isBillable(entity) ? 1.0 : 0.0, "On-demand billed amount");

        journal.recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, databaseTier,
                basePrice.getPricesList().get(0), computeBillableAmount);
        dbPriceList.getConfigurationPriceAdjustmentsList().stream()
                .filter(databaseConfig::matchesPriceTableConfig)
                .findAny()
                .ifPresent(priceAdjustmentConfig -> journal.recordOnDemandCost(
                        CostCategory.ON_DEMAND_LICENSE,
                        databaseTier,
                        priceAdjustmentConfig.getPricesList().get(0), computeBillableAmount));
        if (!dbPriceList.getDependentPricesList().isEmpty()) {
            //add storage price
            journal.recordOnDemandCost(CostCategory.STORAGE, databaseTier,
                    calculateRDBStorageCost(storagePrices, entity, CommodityType.STORAGE_AMOUNT),
                    FULL);
        }
    }

    /**
     * Record dbs prices and add it to the compute cost journal.
     *
     * @param dbsPriceList    DB server list contains all the db prices.
     * @param journal        Journal used to add the costs to.
     * @param dbsTier   DB server Tier that we are calculating.
     * @param databaseConfig DB server config of the db that we want to record.
     * @param entity         current DB server entity.
     */
    private void recordDatabaseServerCost(DatabaseServerTierPriceList dbsPriceList,
            Builder<ENTITY_CLASS> journal, ENTITY_CLASS dbsTier, DatabaseConfig databaseConfig,
            final ENTITY_CLASS entity) {

        final TraxNumber computeBillableAmount = trax(isBillable(entity) ? 1.0 : 0.0, "On-demand billed amount");

        for (DatabaseServerTierConfigPrice serverConfigPrice : dbsPriceList.getConfigPricesList()) {
            final DatabaseTierConfigPrice configPrice =
                    serverConfigPrice.getDatabaseTierConfigPrice();
            if (databaseConfig.matchesPriceTableConfig(configPrice)) {
                journal.recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, dbsTier,
                        configPrice.getPricesList().get(0), computeBillableAmount);
                final Multimap<CommodityType, Price> storagePrices = ArrayListMultimap.create();
                //we support only Storage Amount and IOPS cost, other will be ignored
                for (Price price : serverConfigPrice.getDependentPricesList()) {
                    switch (price.getUnit()) {
                        //storage amount price
                        case GB_MONTH:
                            storagePrices.put(CommodityType.STORAGE_AMOUNT, price);
                            break;
                        //storage IOPS price
                        case MILLION_IOPS:
                            storagePrices.put(CommodityType.STORAGE_ACCESS, price);
                            break;
                    }
                }
                for (Entry<CommodityType, Collection<Price>> entry : storagePrices.asMap().entrySet()){
                    journal.recordOnDemandCost(CostCategory.STORAGE, dbsTier,
                            calculateRDBStorageCost(entry.getValue(), entity,
                                    entry.getKey()), FULL);
                }
            }
        }
    }

    /**
     * Helper Method to calculate dependent storage for DB or DBS cost in a cumulative fashion.
     *
     * @param dependentPricesList List of {@link Price} for various storage amounts.
     * @param entity          current DB entity.
     * @return {@link Price} final price for storage.
     */
    @Nonnull
    private Price calculateRDBStorageCost(@Nonnull final Collection<Price> dependentPricesList,
            @Nonnull final ENTITY_CLASS entity, CommodityType commodityType) {
        final int entityType = entityInfoExtractor.getEntityType(entity);
        final String entityTypeName = EntityType.forNumber(entityType).name();


        final Optional<Float> capacity = entityInfoExtractor.getRDBCommodityCapacity(entity, commodityType);
        final Price defaultPrice = Price.getDefaultInstance();
        if (!capacity.isPresent()) {
            logger.debug("No {} storage capacity found for {}.", entityTypeName, entityInfoExtractor.getName(entity));
            return defaultPrice;
        } else if (dependentPricesList.isEmpty()) {
            logger.warn("No storage prices found for {} of type {}.", entityInfoExtractor.getName(entity), entityTypeName);
            return defaultPrice;
        }
        Unit storageUnit = dependentPricesList.iterator().next().getUnit();
        /*
         * Convert storage bytes from MB to GB; as StorageAmount't UNIT is MB
         * and prices can be in GB_MONTH;
         */
        final float commodityCapacity = storageUnit.equals(Unit.GB_MONTH) ?
                capacity.get() / (float)(GBYTE / MBYTE) : capacity.get();
        TraxNumber totalCost = trax(0.0d, String.format("%s, %s Storage cost", commodityType, entityTypeName));
        float currentSize = 0f;
        final ArrayList<Price> sortedDependentPrices = new ArrayList<>(dependentPricesList);
        sortedDependentPrices.sort(Comparator.comparingLong(Price::getEndRangeInUnits));
       for (Price storagePrice : sortedDependentPrices) {
           if (storagePrice.getIncrementInterval() <= 0) {
               return handleNoIncrementStoragePriceOption(entity, storageUnit, commodityCapacity,
                       totalCost, currentSize, storagePrice);
           } else {
               while (currentSize < storagePrice.getEndRangeInUnits()) {
                   currentSize += storagePrice.getIncrementInterval();
                   String traxDescription = String.format("Storage %s price for incrementInterval %s is %s",
                           commodityType, storagePrice.getIncrementInterval(), storagePrice.getPriceAmount().getAmount());
                   TraxNumber addedStoragePrice = trax(storagePrice.getIncrementInterval() * storagePrice.getPriceAmount().getAmount(),
                           traxDescription);
                   totalCost = totalCost.plus(addedStoragePrice).compute();
                   if (currentSize >= commodityCapacity) {
                       // We have reached storage requirements.
                       logger.trace("Reached expected storage {} with {}. Required was {}.",
                               commodityType, currentSize, commodityCapacity);
                       break;
                   }
               }
           }
           if (currentSize >= commodityCapacity) {
               // We have reached storage requirements.
               logger.trace("Reached expected storage {} with {}. Required was {}.", commodityType, currentSize, commodityCapacity);
               break;
           }
        }
        if (currentSize < commodityCapacity) {
            logger.error("The storage tier was unable to satisfy {}: {}, {} storage requirement."
                    + "This will lead to incorrect cost calculation.", entityTypeName, entityInfoExtractor.getName(entity), commodityType);
        }
        // final calculated storage price.
        return Price.newBuilder().setPriceAmount(CurrencyAmount
                .newBuilder().setAmount(totalCost.getValue()).build())
                .setEndRangeInUnits((long)currentSize)
                .setUnit(storageUnit).build();
    }

    private Price handleNoIncrementStoragePriceOption(final @Nonnull ENTITY_CLASS entity,
                                                      @Nonnull final Unit storageUnit,
                                                      final float commodityCapacity,
                                                      @Nonnull TraxNumber totalCost,
                                                      float currentSize,
                                                      @Nonnull final Price storagePrice) {
        if (currentSize < storagePrice.getEndRangeInUnits()) {
            // increment interval was not set. this means we can extract pricing by
            // directly multiplying current size and unit price.
            currentSize = commodityCapacity;
            String traxDescription = String.format("Setting prices directly as no incrementInterval was set. " +
                            "Current size is %s. Price per %s is %s",
                    currentSize, storagePrice.getUnit(), storagePrice.getPriceAmount().getAmount());
            totalCost = totalCost.plus(storagePrice.getPriceAmount().getAmount()
                    * currentSize, traxDescription).compute();
            // final calculated storage price.
            return Price.newBuilder().setPriceAmount(CurrencyAmount
                    .newBuilder().setAmount(totalCost.getValue()).build())
                    .setEndRangeInUnits((long)currentSize)
                    .setUnit(storageUnit).build();
        } else {
            logger.error("Invalid increment interval for DB : {}. Can not calculate DB storage cost",
                    entityInfoExtractor.getName(entity));
            return Price.getDefaultInstance();
        }
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
    private void recordPriceRangeEntries(final TraxNumber amountToBuy,
                                         @Nonnull final List<Price> prices,
                                         @Nonnull final BiConsumer<Price, TraxNumber> recordFn) {
        if (amountToBuy.getValue() <= 0) {
            return;
        }
        TraxNumber remainingToBuy = amountToBuy;
        long lastPriceRangeEnd = 0;
        for (Price price : prices) {
            // Either buy the entire remaining capacity, or
            // the entire amount available at this range.
            if (price.getEndRangeInUnits() < 0) {
                logger.warn("Illegal negative end range {} for price {}." +
                    "Skipping this price for calculation.", price.getEndRangeInUnits(), price);
            } else {
                // 0 indicates the default/unset value.
                final TraxNumber curEndRange = trax(price.getEndRangeInUnits() == 0 ?
                        Long.MAX_VALUE : price.getEndRangeInUnits(), "end of range");
                final TraxNumber maxAmtAtPrice = curEndRange
                    .minus(lastPriceRangeEnd, "end of last range")
                    .compute("max amt at price");
                final TraxNumber amtBought = Trax.min(remainingToBuy, maxAmtAtPrice)
                    .compute("amount bought " + price.getUnit().name());

                remainingToBuy = remainingToBuy.minus(amtBought).compute("remainder after price tier "
                    + price.getPriceAmount().getAmount());
                lastPriceRangeEnd = price.getEndRangeInUnits();
                if (amtBought.getValue() > 0) {
                    recordFn.accept(price, amtBought);
                    if (remainingToBuy.getValue() <= 0) {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        if (remainingToBuy.getValue() > 0) {
            // If this happens, it means the price range is limited and we are trying to "buy"
            // more than is possible. This shouldn't really happen, because in the cloud you
            // can always buy more.
            logger.error("Bad price list could not accommodate a purchase of {} units. Prices: {}",
                prices);
        }
    }

    /**
     * Checks if entity is billable. Implementation considers only powered on entities as billable.
     *
     * @param entity Entity to check.
     * @return True if entity is billable.
     */
    private boolean isBillable(@Nonnull final ENTITY_CLASS entity) {
        return entityInfoExtractor.getEntityState(entity) == EntityState.POWERED_ON;
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
                    @Nonnull final CloudCostData<ENTITY_CLASS> cloudCostData,
                    @Nonnull final CloudTopology<ENTITY_CLASS> cloudTopology,
                    @Nonnull final EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor,
                    @Nonnull final ReservedInstanceApplicatorFactory<ENTITY_CLASS> riApplicatorFactory,
                    @Nonnull final DependentCostLookup<ENTITY_CLASS> dependentCostLookup,
                    @Nonnull final Map<Long, EntityReservedInstanceCoverage> topologyRICoverage) {
                return new CloudCostCalculator<>(cloudCostData, cloudTopology,
                        entityInfoExtractor, riApplicatorFactory,
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
         * @param riApplicatorFactory Figures out per-entity RI coverage precentage.
         * @param dependentCostLookup Lookup for cost dependencies (e.g. VM -> Volume).
         * @param topologyRICoverage RI coverage information.
         * @return The cost calculator.
         */
        @Nonnull
        CloudCostCalculator<ENTITY_CLASS> newCalculator(
                @Nonnull CloudCostData<ENTITY_CLASS> cloudCostData,
                @Nonnull CloudTopology<ENTITY_CLASS> cloudTopology,
                @Nonnull EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor,
                @Nonnull ReservedInstanceApplicatorFactory<ENTITY_CLASS> riApplicatorFactory,
                @Nonnull DependentCostLookup<ENTITY_CLASS> dependentCostLookup,
                @Nonnull Map<Long, EntityReservedInstanceCoverage> topologyRICoverage);
    }
}
