package com.vmturbo.market.topology.conversions;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.market.topology.RiDiscountedMarketTier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class is the gateway to all cloud topology conversions and mappings between
 * MarketTiers and TraderTOs are stored here
 */
public class CloudTopologyConverter {
    private static final Logger logger = LogManager.getLogger();
    // The mapping between oid of traderTO and MarketTier is stored in this bimap.
    // This is a 1-1 mapping - so one TraderTO will correspond to only one MarketTier and
    // one MarketTier will correspond to only one TraderTO.
    // Two way mapping is needed because ->
    // 1.) When the actions come back from Market, the MarketTier can be looked up based on the
    // oid of the traderTO in the action. The MarketTier will contain the relevant
    // TopologyEntityDTOs like computeTier / region for onDemandMarketTier
    // and tenancy / Avaialbility zone etc for RiDiscountedMarketTier.
    // 2.) When setting the supplier of VM compute shopping list in TopologyConverter,
    // the traderTO to be used as supplier can be looked up based on the MarketTier.
    private final BiMap<Long, MarketTier> traderTOOidToMarketTier = HashBiMap.create();
    // Each type of tier has its own converter which will convert the tier to market tiers.
    private final ComputeTierConverter computeTierConverter;
    private final ReservedInstanceConverter riConverter;
    private final StorageTierConverter storageTierConverter;
    private final Map<Integer, TierConverter> converterMap;
    private TopologyInfo topologyInfo;
    private final BiCliquer pmBasedBicliquer;
    private final BiCliquer dsBasedBicliquer;
    private final CommodityConverter commodityConverter;
    private final Map<Long, TopologyEntityDTO> topology;
    private final Map<TopologyEntityDTO, TopologyEntityDTO> azToRegionMap;
    private final Set<TopologyEntityDTO> businessAccounts;
    private final CloudCostData cloudCostData;
    private Map<Long, AccountPricingData> accountPricingDataByBusinessAccountOid = new HashMap<>();

    /**
     * @param topology the topologyEntityDTOs which came into market-component
     * @param topologyInfo the topology info
     * @param pmBasedBicliquer PM based bicliquer which stores connections between PM and DSs
     * @param dsBasedBicliquer DS based bicliquer which stores connections between DS and PMs
     * @param commodityConverter commodity converter
     * @param azToRegionMap mapping of AZs to Regions
     * @param businessAccounts The set of business accounts
     * @param marketPriceTable The market price table
     * @param cloudCostData Cloud Cost data
     * @param tierExcluder tier exclusion applicator which is used to apply tier
     *                                exclusion settings
     */
     @VisibleForTesting
     CloudTopologyConverter(
             @Nonnull Map<Long, TopologyEntityDTO> topology, @Nonnull TopologyInfo topologyInfo,
             @Nonnull BiCliquer pmBasedBicliquer, @Nonnull BiCliquer dsBasedBicliquer,
             @Nonnull CommodityConverter commodityConverter,
             @Nonnull Map<TopologyEntityDTO, TopologyEntityDTO> azToRegionMap,
             @Nonnull Set<TopologyEntityDTO> businessAccounts, @Nonnull MarketPriceTable marketPriceTable,
             @Nonnull CloudCostData cloudCostData,
             @Nonnull TierExcluder tierExcluder) {
         this.topology = topology;
         this.topologyInfo = topologyInfo;
         this.commodityConverter = commodityConverter;
         this.pmBasedBicliquer = pmBasedBicliquer;
         this.dsBasedBicliquer = dsBasedBicliquer;
         this.azToRegionMap = azToRegionMap;
         CostDTOCreator costDTOCreator = new CostDTOCreator(commodityConverter, marketPriceTable);
         this.computeTierConverter = new ComputeTierConverter(topologyInfo, commodityConverter, costDTOCreator, tierExcluder);
         this.storageTierConverter = new StorageTierConverter(topologyInfo, commodityConverter, costDTOCreator);
         this.riConverter = new ReservedInstanceConverter(topologyInfo, commodityConverter, costDTOCreator, tierExcluder);
         this.businessAccounts = businessAccounts;
         this.cloudCostData = cloudCostData;
         converterMap = Collections.unmodifiableMap(createConverterMap());
     }

    /**
     * Creates traderTOs for all the tier TopologyEntityDTOs in the topology.
     * One topologyEntityDTO tier can map to multiple traderTOs, and each traderTO maps to
     * one marketTier.
     * This method also populates traderTOOidToMarketTier with mappings between oid of traderTO
     * created and MarketTier.
     *
     * @return a list of {@link TraderTO.Builder}
     */
    @Nonnull
    List<TraderTO.Builder> createMarketTierTraderTOs() {
        List<TraderTO.Builder> traderTOBuilders = new ArrayList<>();
        List<TraderTO.Builder> computeMarketTierBuilders = new ArrayList<>();
        logger.info("Beginning creation of market tier trader TOs");
        Set<AccountPricingData> uniqueAccountPricingData = ImmutableSet.copyOf(accountPricingDataByBusinessAccountOid.values());
        for (Entry<Long, TopologyEntityDTO> entry : checkNotNull(topology.entrySet())) {
            TopologyEntityDTO entity = entry.getValue();
            TierConverter converter = converterMap.get(entity.getEntityType());
            if (converter != null) {
                Map<TraderTO.Builder, MarketTier> traderTOBuildersForEntity =
                        converter.createMarketTierTraderTOs(entity, topology, businessAccounts, uniqueAccountPricingData);
                traderTOBuilders.addAll(traderTOBuildersForEntity.keySet());
                if (entity.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
                    computeMarketTierBuilders.addAll(traderTOBuildersForEntity.keySet());
                }
                // Add all the traderTO oids to MarketTier mappings to
                // traderTOOidToMarketTier
                traderTOBuildersForEntity.forEach((traderTO, marketTier) ->
                        traderTOOidToMarketTier.put(traderTO.getOid(), marketTier));
            }
        }

        // create TOs for RiDiscountedMarketTiers
        // since riData does not come along with the topologyEntityDTOs, RiDiscountedMarketTier creation
        // happens outside the for loop processing topologyEntityDTOs
        Map<TraderTO.Builder, MarketTier> traderTOBuildersForRis =
                riConverter.createMarketTierTraderTOs(cloudCostData, topology, businessAccounts);
        traderTOBuilders.addAll(traderTOBuildersForRis.keySet());
        computeMarketTierBuilders.addAll(traderTOBuildersForRis.keySet());
        // Add all the traderTO oids to MarketTier mappings to
        // traderTOOidToMarketTier
        traderTOBuildersForRis.forEach((traderTO, marketTier) ->
                traderTOOidToMarketTier.put(traderTO.getOid(), marketTier));

        // After all the market tiers are constructed, populate the bicliquers
        for(TraderTO.Builder computeMarketTierBuilder : computeMarketTierBuilders) {
            MarketTier marketTier = traderTOOidToMarketTier.get(computeMarketTierBuilder.getOid());
            List<MarketTier> storageMarketTiers = marketTier.getConnectedMarketTiersOfType(
                    EntityType.STORAGE_TIER_VALUE, topology);
            for(MarketTier storageMarketTier : storageMarketTiers) {
                Long storageOid = traderTOOidToMarketTier.inverse().get(storageMarketTier);
                if(storageOid != null) {
                    pmBasedBicliquer.edge(String.valueOf(computeMarketTierBuilder.getOid()),
                            String.valueOf(storageOid));
                    dsBasedBicliquer.edge(String.valueOf(storageOid),
                            String.valueOf(computeMarketTierBuilder.getOid()));
                }
            }
        }
        logger.info("Completed creation of market tier trader TOs");
        return traderTOBuilders;
    }

    /**
     * Get the traderTO oid corresponding to the MarketTier.
     *
     * @param marketTier the MarketTier for which the corresponding
     *                          traderTO oid will be found.
     * @return traderTO oid
     */
    @Nullable
    Long getTraderTOOid(@Nonnull MarketTier marketTier) {
        return traderTOOidToMarketTier.inverse().get(checkNotNull(marketTier));
    }

    /**
     * Get the MarketTier corresponding to the traderTO oid.
     *
     * @param traderToOid the traderToOid for which the corresponding MarketTier
     *                    will be found.
     * @return {@link MarketTier} which corresponds to the traderTO oid
     */
    @Nullable
    MarketTier getMarketTier(long traderToOid) {
        return traderTOOidToMarketTier.get(traderToOid);
    }

    /**
     * Return value if there is a DiscountedMarketTier corresponding to the traderTOOid
     *
     * @param traderToOid the traderToOid for which the corresponding RiDiscountedMarketTier
     *                    will be found.
     * @return {@link MarketTier} which corresponds to the traderTO oid
     */
    @Nullable
    Optional<MarketTier> getRiDiscountedMarketTier(long traderToOid) {
        MarketTier mt = getMarketTier(traderToOid);
        if (mt != null && mt.hasRIDiscount()) {
            return Optional.of(mt);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Populate the converterMap with mapping between integer representing entity type and the
     * corresponding TierConverter.
     */
    @Nonnull
    private Map<Integer, TierConverter> createConverterMap() {
        Map<Integer, TierConverter> tierConverterMap = new HashMap<>();
        tierConverterMap.put(EntityType.COMPUTE_TIER_VALUE, computeTierConverter);
        tierConverterMap.put(EntityType.STORAGE_TIER_VALUE, storageTierConverter);
        tierConverterMap.put(EntityType.DATABASE_TIER_VALUE, computeTierConverter);
        tierConverterMap.put(EntityType.DATABASE_SERVER_TIER_VALUE, computeTierConverter);
        return tierConverterMap;
    }

    /**
     * Gets the oids of traderTOs representing market tiers of type tierType which are providers
     * for entity.
     * For ex. if a VM1 is passed in as the entity and providerType as
     * EntityType.STORAGE_TIER_VALUE. VM1 is placed on AZ1 which is in Region1 and is connected to
     * 2 storage tiers - st1 and st2. This method will return oids of traders representing
     * [st1 x Region1] and [st2 x Region2].
     *
     * @param entity the {@link TopologyEntityDTO} for which the market tier providers are desired.
     * @param providerType The type of tier
     * @return the oids of traderTOs which represent the marketTier providers of type tierType
     */
    @Nonnull
    Set<Long> getMarketTierProviderOidOfType(@Nonnull TopologyEntityDTO entity, int providerType) {
        Set<Long> providerOids =  new HashSet<>();
        if (!TopologyDTOUtil.isTierEntityType(providerType)) {
            logger.error("{} is not a tier. Cannot fetch market tier providers for {}"
                    , providerType, entity.getDisplayName());
            return providerOids;
        }
        Set<TopologyEntityDTO> connectedEntities =
                getTopologyEntityDTOProvidersOfType(entity, providerType);
        if (connectedEntities.size() == 0) {
            return providerOids;
        }
        TopologyEntityDTO region = getRegionOfCloudConsumer(entity);
        if (region == null) {
            logger.error("{} is not connected to any region.", entity.getDisplayName());
            return providerOids;
        }
        for (TopologyEntityDTO connectedEntity : connectedEntities) {
            MarketTier provider = null;
            provider = new OnDemandMarketTier(connectedEntity);
            Long oid = getTraderTOOid(provider);
            if (oid != null) {
                providerOids.add(oid);
            }
        }
        return providerOids;
    }

    /**
     * Gets the provider TopologyEntityDTOs of a specified type.
     * For ex. if a VM1 is passed in as the entity and providerType as
     * EntityType.STORAGE_TIER_VALUE. VM1 consumes from 2 storage tiers
     * - st1 and st2. This method will return st1 and st2 TopologyEntityDTOs.
     *
     * @param entity the {@link TopologyEntityDTO} for which the providers are desired
     * @param providerType the type of provider to look for
     * @return A set of provider TopologyEntityDTOs.
     */
    @Nonnull
    Set<TopologyEntityDTO> getTopologyEntityDTOProvidersOfType(
            @Nonnull TopologyEntityDTO entity, int providerType) {
        return entity.getCommoditiesBoughtFromProvidersList().stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderEntityType)
                .filter(commBought -> commBought.getProviderEntityType() == providerType)
                .filter(Objects::nonNull)
                .map(commBought -> commBought.getProviderId())
                .map(providerOid -> topology.get(providerOid))
                .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Is the trader consuming from a market tier?
     * @param trader the {@link TraderTO} which is to be checked if it is consuming from a market tier
     * @return True if the trader is consuming from a market tier. False otherwise.
     */
    boolean isTraderConsumingFromMaketTier(TraderTO trader) {
        return trader.getShoppingListsList().stream().anyMatch(
                sl -> traderTOOidToMarketTier.containsKey(sl.getSupplier()));
    }

    /**
     * Gets the primary market tier(compute tier or database tier) which is a supplier
     * for the trader.
     *
     * @return Optional of the primary market tier
     */
    @Nullable
    MarketTier getPrimaryMarketTier(TraderTO trader) {
        List<MarketTier> primaryMarketTiers =  trader.getShoppingListsList().stream()
                .map(sl -> traderTOOidToMarketTier.get(sl.getSupplier()))
                .filter(Objects::nonNull)
                .filter(mTier -> TopologyDTOUtil.isPrimaryTierEntityType(
                        mTier.getTier().getEntityType()))
                .collect(Collectors.toList());
        if (primaryMarketTiers.size() != 1) {
            logger.error("Trader {} is connected to {} primary tiers - {}",
                    trader.getDebugInfoNeverUseInCode(), primaryMarketTiers.size(),
                    primaryMarketTiers.stream().map(t ->
                            t.getDisplayName()).collect(Collectors.joining(",")));
            return null;
        }
        return primaryMarketTiers.get(0);
    }

    /**
     * Gets the primary market tier(compute tier) which is a supplier
     * for the trader.
     *
     * @param trader TraderTO.
     * @return Optional of the primary market tier
     */
    @Nullable
    Optional<MarketTier> getComputeTier(TraderTO trader) {
        List<MarketTier> primaryMarketTiers =  trader.getShoppingListsList().stream()
                .map(sl -> traderTOOidToMarketTier.get(sl.getSupplier()))
                .filter(Objects::nonNull)
                .filter(mTier -> EntityType.COMPUTE_TIER_VALUE == mTier.getTier().getEntityType())
                .collect(Collectors.toList());
        if (primaryMarketTiers.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(primaryMarketTiers.get(0));
    }

    /**
     * Given a trader, get the region comm type from it shopping list.
     *
     * @param trader the trader
     * @return the Integer corresponding to the region comm type
     */
    public Long getRegionCommTypeIntFromShoppingList(TraderTO trader) {
        Optional<ShoppingListTO> shoppingListTO = trader.getShoppingListsList().stream().filter(s -> s.hasContext()).findFirst();
        if (shoppingListTO.isPresent()) {
            return shoppingListTO.get().getContext().getRegionId();
        }
        return null;
    }

    /**
     * Gets the region the entity is placed on. This method will work for any entity placed
     * on an Availability zone like VM / Database.
     *
     * @param entity the {@link TopologyEntityDTO} entity whose connected region is retrieved.
     * @return the region {@link TopologyEntityDTO}
     */
    @Nullable
    TopologyEntityDTO getRegionOfCloudConsumer(@Nonnull TopologyEntityDTO entity) {
        List<TopologyEntityDTO> regions = TopologyDTOUtil.getConnectedEntitiesOfType(
            entity, EntityType.REGION_VALUE, topology);
        // For Azure, a VM is directly connected to Region. It's not connected to Availability Zone.
        // So if an entity is connected to Region, then return this region.
        // Otherwise, find the Region through Availability Zone.
        if (!regions.isEmpty()) {
            return regions.get(0);
        }

        TopologyEntityDTO region = null;
        if (azToRegionMap.isEmpty()) {
            logger.error("azToRegionMap not yet initialized.");
            return region;
        }
        TopologyEntityDTO az = getAZOfCloudConsumer(entity);
        if (az != null) {
            region = azToRegionMap.get(az);
        }
        return region;
    }

    /**
     * Gets the AZ connected to a cloud consumer.
     *
     * @param entity the {@link TopologyEntityDTO}
     * @return the Availability Zone
     */
    @Nullable
    TopologyEntityDTO getAZOfCloudConsumer(@Nonnull TopologyEntityDTO entity) {
        List<TopologyEntityDTO> AZs = TopologyDTOUtil.getConnectedEntitiesOfType(entity,
                EntityType.AVAILABILITY_ZONE_VALUE, topology);
        if (AZs.isEmpty()) {
            logger.debug("{} not connected to any AZs", entity.getDisplayName());
            return null;
        }
        if (AZs.size() > 1) {
            logger.error("{} is connected to {} availability zones - {}", entity.getDisplayName(),
                    AZs.size(), AZs.stream().map(TopologyEntityDTO::getDisplayName)
                            .collect(Collectors.joining(",")));
            return null;
        }
        return AZs.get(0);
    }

    /**
     * Is providerOid the oid of a traderTO representing a market tier?
     *
     * @param providerOid
     * @return true if providerOid is the oid of a traderTO representing a market tier.
     * False otherwise.
     */
    boolean isMarketTier(@Nullable Long providerOid) {
        if (providerOid == null) {
            return false;
        }
        return getMarketTier(providerOid) != null;
    }

    /**
     * Returns {@link ReservedInstanceData} for a given riId
     *
     * @param riId
     * @return ReservedInstanceData corresponding to that riId
     */
    public ReservedInstanceData getRiDataById(long riId) {
        return riConverter.getRiDataById(riId);
    }

    /**
     * Returns {@link EntityReservedInstanceCoverage} of an entity
     *
     * @param entityId
     * @return EntityReservedInstanceCoverage corresponding to the given entity
     */
    public Optional<EntityReservedInstanceCoverage> getRiCoverageForEntity(long entityId) {
        return cloudCostData.getRiCoverageForEntity(entityId);
    }

    /**
     * Get the AccountPricing Id from the business account.
     *
     * @param businessAccountOid The business account oid.
     *
     * @return An optional of AccountPricingData
     */
    public Optional<AccountPricingData> getAccountPricingIdFromBusinessAccount(Long businessAccountOid) {
        return cloudCostData.getAccountPricingData(businessAccountOid);
    }

    /**
     * Populate the accountPricingDataByBusinessAccountOid map.
     *
     * @param businessAccountOid The business account oid.
     * @param accountPricingData The account pricing data.
     */
    public void insertIntoAccountPricingDataByBusinessAccountOidMap(Long businessAccountOid,
                                                                    AccountPricingData accountPricingData) {
        accountPricingDataByBusinessAccountOid.put(businessAccountOid, accountPricingData);
    }
}
