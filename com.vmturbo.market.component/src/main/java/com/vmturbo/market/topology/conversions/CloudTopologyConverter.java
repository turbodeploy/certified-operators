package com.vmturbo.market.topology.conversions;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.RiDiscountedMarketTier;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
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
    private final BiCliquer pmBasedBicliquer;
    private final BiCliquer dsBasedBicliquer;
    private final Map<Long, TopologyEntityDTO> topology;
    private final Map<TopologyEntityDTO, TopologyEntityDTO> azToRegionMap;
    private final Set<TopologyEntityDTO> businessAccounts;
    private final CloudCostData<TopologyEntityDTO> cloudCostData;
    private Map<Long, AccountPricingData> accountPricingDataByBusinessAccountOid = new HashMap<>();
    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    /**
     * @param topology the topologyEntityDTOs which came into market-component
     * @param topologyInfo the topology info
     * @param pmBasedBicliquer PM based bicliquer which stores connections between PM and DSs
     * @param dsBasedBicliquer DS based bicliquer which stores connections between DS and PMs
     * @param commodityConverter commodity converter
     * @param azToRegionMap mapping of AZs to Regions
     * @param businessAccounts The set of business accounts
     * @param marketCloudRateExtractor The market price table
     * @param cloudCostData Cloud Cost data
     * @param tierExcluder tier exclusion applicator which is used to apply tier
     *                                exclusion settings
     * @param cloudTopology instance to look up topology relationships
     */
     @VisibleForTesting
     CloudTopologyConverter(
             @Nonnull Map<Long, TopologyEntityDTO> topology,
             @Nonnull TopologyInfo topologyInfo,
             @Nonnull BiCliquer pmBasedBicliquer,
             @Nonnull BiCliquer dsBasedBicliquer,
             @Nonnull CommodityConverter commodityConverter,
             @Nonnull Map<TopologyEntityDTO, TopologyEntityDTO> azToRegionMap,
             @Nonnull Set<TopologyEntityDTO> businessAccounts,
             @Nonnull CloudRateExtractor marketCloudRateExtractor,
             @Nonnull CloudCostData cloudCostData,
             @Nonnull TierExcluder tierExcluder,
             @Nullable CloudTopology<TopologyEntityDTO> cloudTopology) {
         this.topology = topology;
         this.pmBasedBicliquer = pmBasedBicliquer;
         this.dsBasedBicliquer = dsBasedBicliquer;
         this.azToRegionMap = azToRegionMap;
         CostDTOCreator costDTOCreator = new CostDTOCreator(commodityConverter,
                 marketCloudRateExtractor);
         this.computeTierConverter = new ComputeTierConverter(topologyInfo, commodityConverter, costDTOCreator, tierExcluder);
         this.storageTierConverter = new StorageTierConverter(topologyInfo, commodityConverter, costDTOCreator, tierExcluder);
         this.riConverter = new ReservedInstanceConverter(topologyInfo, commodityConverter,
                 costDTOCreator, tierExcluder, cloudTopology);
         this.businessAccounts = businessAccounts;
         this.cloudCostData = cloudCostData;
         converterMap = Collections.unmodifiableMap(createConverterMap());
         this.cloudTopology = cloudTopology;
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
                riConverter.createMarketTierTraderTOs(cloudCostData, topology, accountPricingDataByBusinessAccountOid);
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
     * Returns the {@link Context} corresponding to the destination region in the context of cloud migration.
     *
     * @param regionPresentContexts a list of {@link Context} with a region attribute present
     * @param sourceRegion source region if one is present
     * @return the destination context
     */
    @Nullable
    private Context getDestinationContext(
            @Nonnull final List<Context> regionPresentContexts,
            @Nullable final TopologyEntityDTO sourceRegion) {
        if (Objects.nonNull(sourceRegion)) {
            Optional<Context> regionPresentContextOptional = regionPresentContexts.stream()
                    .filter(context -> context.getRegionId() != sourceRegion.getOid())
                    .findFirst();
            return regionPresentContextOptional.orElse(null);
        } else {
            return regionPresentContexts.get(0);
        }
    }

    /**
     * Creates a map of businessAccount OID -> set of new {@link ConnectedEntity} objects given MCP results. Since
     * businessAccounts do not participate in the market as traders, these connections are not automatically created.
     * However, they are necessary for cost computations associated with entities that were previously either:
     * 1.) On-prem
     * 2.) Hosted by another CSP
     *
     * @param traderTOs {@link TraderTO} analysis results
     * @param originalTopology a map of OID to {@link TopologyEntityDTO} modeling the original topology
     * @param businessAccountIdToTopologyEntityDTO a map of businessAccount OID -> {@link TopologyEntityDTO}
     * @return a map of businessAccount OID -> set of new {@link ConnectedEntity} objects with which it should be associated
     */
    @Nonnull
    public Map<Long, Set<ConnectedEntity>> getBusinessAccountsToNewlyOwnedEntities(
            @Nonnull final List<TraderTO> traderTOs,
            @Nonnull final Map<Long, TopologyEntityDTO> originalTopology,
            @Nonnull final Map<Long, TopologyEntityDTO> businessAccountIdToTopologyEntityDTO) {
        final Map<Long, TopologyEntityDTO> balanceAccountIdToBusinessAccount =
                getBalanceAccountIdToBusinessAccount(businessAccountIdToTopologyEntityDTO);
        final Map<Long, Set<ConnectedEntity>> businessAccountToNewlyOwnedEntities = Maps.newHashMap();
        for (TraderTO traderTO : traderTOs) {
            final long traderOid = traderTO.getOid();
            if (isMarketTier(traderOid)) {
                continue;
            }
            final TopologyEntityDTO originalEntity = originalTopology.get(traderOid);
            final TopologyEntityDTO sourceRegion = getRegionOfCloudConsumer(originalEntity);
            final List<Context> regionPresentContexts = getContextsWithRegionPresent(traderTO);
            if (CollectionUtils.isEmpty(regionPresentContexts)) {
                continue;
            }
            final boolean isCloudToCloudMigration = Objects.nonNull(sourceRegion)
                    && regionPresentContexts.size() > 1;
            // IF Migrating from on-prem OR
            // Migrating from region A to region B
            // connect BA -> previously on-prem (or located on another CSP/BA) workload via OWNS connection
            if (Objects.isNull(sourceRegion) || isCloudToCloudMigration) {
                final Context destinationContext = getDestinationContext(regionPresentContexts, sourceRegion);
                if (Objects.isNull(destinationContext)) {
                    logger.error("Cloud migration TraderTO {} ShoppingLists do not contain a non-source region ID- skipping this trader", traderOid);
                    continue;
                }
                final TopologyEntityDTO businessAccount = balanceAccountIdToBusinessAccount.get(
                        destinationContext.getBalanceAccount().getId());
                if (Objects.isNull(businessAccount)) {
                    continue;
                }
                final long businessAccountOid = businessAccount.getOid();
                final ConnectedEntity newlyOwnedEntity = ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectedEntity.ConnectionType.OWNS_CONNECTION)
                        .setConnectedEntityType(traderTO.getType())
                        .setConnectedEntityId(traderTO.getOid()).build();
                TopologyEntityDTO sourceEntity = originalTopology.get(traderOid);
                List<Long> volumeIds = new ArrayList<>();
                if (sourceEntity != null && sourceEntity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                    // If provider is a virtual volume, get volume ID from the provider ID.
                    volumeIds.addAll(sourceEntity.getCommoditiesBoughtFromProvidersList().stream()
                            .filter(commList -> commList.getProviderEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                            .map(CommoditiesBoughtFromProvider::getProviderId)
                            .collect(Collectors.toList()));
                    // If provider is a storage (on-prem), get the volume ID from the volumeId field.
                    volumeIds.addAll(sourceEntity.getCommoditiesBoughtFromProvidersList().stream()
                            .filter(commList -> commList.getProviderEntityType() == EntityType.STORAGE_VALUE)
                            .map(CommoditiesBoughtFromProvider::getVolumeId)
                            .collect(Collectors.toList()));
                }
                if (businessAccountToNewlyOwnedEntities.containsKey(businessAccountOid)) {
                    businessAccountToNewlyOwnedEntities
                            .get(businessAccountOid)
                            .add(newlyOwnedEntity);
                } else {
                    businessAccountToNewlyOwnedEntities.put(businessAccountOid, Sets.newHashSet(newlyOwnedEntity));
                }
                volumeIds.forEach(id -> {
                    final ConnectedEntity connectedVolume = ConnectedEntity.newBuilder()
                            .setConnectionType(ConnectedEntity.ConnectionType.OWNS_CONNECTION)
                            .setConnectedEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                            .setConnectedEntityId(id).build();
                    businessAccountToNewlyOwnedEntities
                            .get(businessAccountOid)
                            .add(connectedVolume);
                });
            }
        }
        return businessAccountToNewlyOwnedEntities;
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

    @Nonnull
    Optional<RiDiscountedMarketTier> getRIDiscountedMarketTierFromRIData(@Nonnull ReservedInstanceData riData) {
        return riConverter.getMarketTierForRIData(riData);
    }

    /**
     * find the oid of the RIDiscountedMarketTier for the given riData.
     *
     * @param riData given RI data.
     * @return the oid of the RIDiscountedMarketTier.
     */
    @Nullable
    public Long getRIDiscountedMarketTierIDFromRIData(ReservedInstanceData riData) {

        return getRIDiscountedMarketTierFromRIData(riData)
                .map(this::getTraderTOOid)
                .orElse(null);
    }

    /**
     * Get the MarketTier corresponding to the traderTO oid.
     *
     * @param traderToOid the traderToOid for which the corresponding MarketTier
     *                    will be found.
     * @return {@link MarketTier} which corresponds to the traderTO oid
     */
    @Nullable
    public MarketTier getMarketTier(long traderToOid) {
        return traderTOOidToMarketTier.get(traderToOid);
    }

    /**
     * Get the source/destination tier id of a MoveTO.
     * @param moveTO the moveTO
     * @param targetEntityId the consumer entity id
     * @param isSource true if you need the source tier id. false if you need the destination tier id.
     * @return Optional of the source/destination tier id. If the Move is not a cloud entities,
     *         then optional empty will be returned. If the source is an RI, we can still find the
     *         source tier from the orignal topology. But if the destination tier is an RI, we cannot
     *         find the destination tier and we return Optional.empty
     */
    public Optional<Long> getSourceOrDestinationTierFromMoveTo(MoveTO moveTO, long targetEntityId, boolean isSource) {
        Long tierId = null;
        long marketTierId = isSource ? moveTO.getSource() : moveTO.getDestination();
        MarketTier marketTier = getMarketTier(marketTierId);
        if (marketTier != null) {
            if (isSource) {
                if (marketTier.hasRIDiscount()) {
                    TopologyEntityDTO entity = topology.get(targetEntityId);
                    if (entity != null) {
                        Optional<CommoditiesBoughtFromProvider> primaryCommBoughtgrouping =
                            entity.getCommoditiesBoughtFromProvidersList().stream()
                                .filter(grouping -> grouping.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE)
                                .findFirst();
                        if (primaryCommBoughtgrouping.isPresent()) {
                            tierId = primaryCommBoughtgrouping.get().getProviderId();
                        }
                    }
                } else {
                    tierId = marketTier.getTier().getOid();
                }
            } else {
                if (!marketTier.hasRIDiscount()) {
                    tierId = marketTier.getTier().getOid();
                } else {
                    logger.warn("Cannot find tier from RIMarkerTier {}", marketTier.getDisplayName());
                }
            }
        }
        return Optional.ofNullable(tierId);
    }

    /**
     * Gets instance of cloud topology.
     *
     * @return Can be null, otherwise CloudTopology instance.
     */
    @Nullable
    public CloudTopology<TopologyEntityDTO> getCloudTopology() {
        return this.cloudTopology;
    }

    /**
     * Return value if there is a DiscountedMarketTier corresponding to the traderTOOid.
     *
     * @param traderToOid the traderToOid for which the corresponding RiDiscountedMarketTier
     *                    will be found.
     * @return {@link MarketTier} which corresponds to the traderTO oid.
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
            MarketTier provider = new OnDemandMarketTier(connectedEntity);
            Long oid = getTraderTOOid(provider);
            if (oid != null) {
                providerOids.add(oid);
            }
        }
        return providerOids;
    }

    /**
     * Gets the provider TopologyEntityDTOs of a specified type.
     *
     * @param entity the {@link TopologyEntityDTO} for which the providers are desired
     * @param providerType the type of provider to look for
     * @return A set of provider TopologyEntityDTOs.
     */
    @Nonnull
    Set<TopologyEntityDTO> getTopologyEntityDTOProvidersOfType(
            @Nonnull TopologyEntityDTO entity, int providerType) {
        final Set<TopologyEntityDTO> providers =
                getTopologyEntityDTODirectProvidersOfType(Sets.newHashSet(entity), providerType);
        if (!providers.isEmpty()) {
            return providers;
        }
        final Integer collapsedEntityType = CollapsedTraderHelper.getCollapsedEntityType(entity.getEntityType());
        // If there is collapsed entity between entity and providerType, first get collapsed entity,
        // then get providers with providerType from collapsed entity.
        // If a cloud VM is passed in as the entity and EntityType.STORAGE_TIER_VALUE as providerType,
        // returns the storageTier providers for the volumes that the VM resides on. For example,
        // given that VM1 consumes from vol1, and vol1 consumes from storageTier1, with input VM1 and
        // EntityType.STORAGE_TIER_VALUE, this method will return storageTier1 TopologyEntityDTO.
        if (collapsedEntityType != null) {
            Set<TopologyEntityDTO> directProviders =
                    getTopologyEntityDTODirectProvidersOfType(Sets.newHashSet(entity), collapsedEntityType);
            return getTopologyEntityDTODirectProvidersOfType(directProviders, providerType);
        }
        return providers;
    }

    /**
     * Gets the direct provider TopologyEntityDTOs of a specified type.
     *
     * @param entities entities for which the providers are desired
     * @param providerType the type of provider to look for
     * @return A set of provider TopologyEntityDTOs.
     */
    private Set<TopologyEntityDTO> getTopologyEntityDTODirectProvidersOfType(@Nonnull Set<TopologyEntityDTO> entities,
                                                                             int providerType) {
        return entities.stream()
                .flatMap(e -> e.getCommoditiesBoughtFromProvidersList().stream())
                .filter(CommoditiesBoughtFromProvider::hasProviderEntityType)
                .filter(commBought -> commBought.getProviderEntityType() == providerType)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .map(topology::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
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
                    primaryMarketTiers.stream()
                            .map(MarketTier::getDisplayName)
                            .collect(Collectors.joining(",")));
            return null;
        }
        return primaryMarketTiers.get(0);
    }

    /**
     * Get the index of the compute shopping list for the trader.
     * @param trader the trader of interest.
     * @return index of the compute shopping list. -1 if there is none.
     */
    int getIndexOfSlSuppliedByPrimaryTier(TraderTO trader) {
        for (int i = 0; i < trader.getShoppingListsList().size(); i++) {
            ShoppingListTO sl = trader.getShoppingListsList().get(i);
            MarketTier mTier = traderTOOidToMarketTier.get(sl.getSupplier());
            if (mTier != null && TopologyDTOUtil.isPrimaryTierEntityType(
                    mTier.getTier().getEntityType())) {
                return i;
            }
        }
        return -1;
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
     * Determines a trader's reserved instance coverage capacity. If the trader is connected to a
     * compute tier, its capacity with either mirror the capacity of the compute tier or it will be
     * 0, based on {@code traderState}.
     * @param trader The target {@link TraderTO}, expected to be a workload (VM, DB, DBS).
     * @param traderState The trader's state. This is accepted as a separate argument from the trader
     *                    due to projected {@link TraderTO} instances always having a state of ACTIVE,
     *                    which is not indicative of their power state
     * @return If the trader is connected to a compute tier, its coverage capacity based on the
     * {@code traderState}. If the trader is not connected to a compute tier, returns {@link Optional#empty()}.
     */
    @Nonnull
    public Optional<Integer> getReservedInstanceCoverageCapacity(@Nonnull TraderTO trader,
                                                                 @Nonnull TraderStateTO traderState) {
        return getComputeTier(trader)
                .map(MarketTier::getTier)
                .map(TopologyEntityDTO::getTypeSpecificInfo)
                .map(TypeSpecificInfo::getComputeTier)
                .map(computeTierInfo -> traderState == TraderStateTO.ACTIVE ?
                        computeTierInfo.getNumCoupons() : 0);
    }

    /**
     * Given a trader, get the first {@link Context} object from shopping list with a region present.
     *
     * @param trader the trader
     * @return the Integer corresponding to the region comm type
     */
    public Context getContextWithRegionPresent(TraderTO trader) {
        List<Context> contextsWithRegionPresent = getContextsWithRegionPresent(trader);
        if (CollectionUtils.isEmpty(contextsWithRegionPresent)) {
            return null;
        }
        return contextsWithRegionPresent.get(0);
    }

    /**
     * Given a trader, get all {@link Context} objects from the shopping list with regions present.
     *
     *
     * @param trader the {@link TraderTO} from which a region-present context should be extracted
     * @return a list of {@link Context} where a region is referenced
     */
    public List<Context> getContextsWithRegionPresent(TraderTO trader) {
        return trader.getShoppingListsList()
                .stream()
                .filter(shoppingList -> shoppingList.hasContext() && shoppingList.getContext().hasRegionId())
                .map(shoppingList -> shoppingList.getContext())
                .collect(Collectors.toList());
    }

    /**
     * Gets the region the entity is placed on. This method will work for any entity placed
     * on an Availability zone like VM / Database.
     *
     * @param entity the {@link TopologyEntityDTO} entity whose connected region is retrieved.
     * @return the region {@link TopologyEntityDTO}
     */
    @Nullable
    public TopologyEntityDTO getRegionOfCloudConsumer(@Nonnull TopologyEntityDTO entity) {
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
     * @param providerOid the OID of the provider
     * @return true if providerOid is the oid of a traderTO representing a market tier.
     * False otherwise.
     */
    public boolean isMarketTier(@Nullable Long providerOid) {
        if (providerOid == null) {
            return false;
        }
        return getMarketTier(providerOid) != null;
    }

    /**
     * Get the {@link ReservedInstanceData} by id.
     *
     * @param reservedInstanceId reserved instance id
     * @return the {@link ReservedInstanceData}
     */
    @Nullable
    public ReservedInstanceData getRiDataById(long reservedInstanceId) {
        return riConverter.getRiDataById(reservedInstanceId);
    }

    /**
     * Returns {@link EntityReservedInstanceCoverage} of an entity
     *
     * @param entityId
     * @return EntityReservedInstanceCoverage corresponding to the given entity
     */
    public Optional<EntityReservedInstanceCoverage> getRiCoverageForEntity(long entityId) {
        return cloudCostData.getFilteredRiCoverage(entityId);
    }

    /**
     * Get the AccountPricing Id from the business account.
     *
     * @param businessAccountOid The business account oid.
     *
     * @return An optional of AccountPricingData
     */
    public Optional<AccountPricingData<TopologyEntityDTO>> getAccountPricingIdFromBusinessAccount(
            Long businessAccountOid) {
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

    /**
     * Given a map of OID to {@link TopologyEntityDTO} representing business accounts in the topology, returns a map of
     * {@link AccountPricingData} OID -> businessAccountRepresented {@link TopologyEntityDTO}. This is used to connect
     * projected topological entities to the billing families they are projected to belong to- if more than one business
     * account corresponds to a given billing family, the first one is represented.
     *
     * @param businessAccountIdToTopologyEntityDTO a map of businessAccount OID -> {@link TopologyEntityDTO}
     * @return a map of balanceAccountOid -> businessAccount {@link TopologyEntityDTO}
     */
    public Map<Long, TopologyEntityDTO> getBalanceAccountIdToBusinessAccount(Map<Long, TopologyEntityDTO> businessAccountIdToTopologyEntityDTO) {
        return accountPricingDataByBusinessAccountOid.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getValue().getAccountPricingDataOid(),
                        entry -> businessAccountIdToTopologyEntityDTO.get(entry.getKey()),
                        BinaryOperator.minBy(Comparator.comparingLong(TopologyEntityDTO::getOid))));
    }
}
