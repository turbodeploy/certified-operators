package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.market.runner.ReservedCapacityAnalysis;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.settings.EntitySettings;
import com.vmturbo.market.settings.MarketSettings;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.RiDiscountedMarketTier;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.market.topology.conversions.CommodityIndex.CommodityIndexFactory;
import com.vmturbo.market.topology.conversions.ConversionErrorCounts.ErrorCategory;
import com.vmturbo.market.topology.conversions.ConversionErrorCounts.Phase;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults.NewShoppingListToBuyerEntry;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessagePayload;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.SumOfCommodity;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Convert topology DTOs to economy DTOs.
 */
public class TopologyConverter {

    public static final Set<TopologyDTO.EntityState> SKIPPED_ENTITY_STATES = ImmutableSet.of(
            TopologyDTO.EntityState.UNKNOWN,
            TopologyDTO.EntityState.MAINTENANCE,
            TopologyDTO.EntityState.FAILOVER);

    private static final boolean INCLUDE_GUARANTEED_BUYER_DEFAULT =
            MarketSettings.BooleanKey.INCLUDE_GUARANTEED_BUYER.value();

    // Key is the type of original entity, value is a set of types
    // Copy the connected entities of original entity into the projected entity except for the
    // ones in the map, which need to be computed like Availability zone or Region
    // because the AZ or Region might have changed.
    private Map<Integer, Set<Integer>> projectedConnectedEntityTypesToCompute = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE,
                    ImmutableSet.of(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE),
            EntityType.DATABASE_VALUE, ImmutableSet.of(EntityType.AVAILABILITY_ZONE_VALUE),
            EntityType.DATABASE_SERVER_VALUE, ImmutableSet.of(EntityType.AVAILABILITY_ZONE_VALUE));

    private static final Set<Integer> CONTAINER_TYPES = ImmutableSet.of(
        // TODO: Add container collection
        EntityType.CONTAINER_VALUE);

    // if cloud entity buys from this set of cloud entity types, then we need to create DataCenter
    // commodity bought for it
    private static final Set<Integer> CLOUD_ENTITY_TYPES_TO_CREATE_DC_COMM_BOUGHT = ImmutableSet.of(
            EntityType.COMPUTE_TIER_VALUE,
            EntityType.DATABASE_TIER_VALUE,
            EntityType.DATABASE_SERVER_TIER_VALUE
    );

    private static final Logger logger = LogManager.getLogger();

    // TODO: In legacy this is taken from LicenseManager and is currently false
    private boolean includeGuaranteedBuyer = INCLUDE_GUARANTEED_BUYER_DEFAULT;

    private CloudTopologyConverter cloudTc;

    /**
     * Entities that are providers of containers.
     * Populated only for plans. For realtime market, this set will be empty.
     */
    private Set<Long> providersOfContainers = Sets.newHashSet();

    // Store skipped service entities which need to be added back to projected topology and price
    // index messages.
    private Map<Long, TopologyEntityDTO> skippedEntities = Maps.newHashMap();

    // a map keeps shoppinglist oid to ShoppingListInfo which is a container for
    // shoppinglist oid, buyer oid, seller oid and commodity bought
    private final Map<Long, ShoppingListInfo> shoppingListOidToInfos = Maps.newHashMap();

    // We need access only in tests
    protected final Map<Long, ShoppingListInfo> getShoppingListOidToInfos() {return shoppingListOidToInfos;}

    // Mapping of CommoditySpecificationTO (string representation of type and baseType from
    // CommoditySpecificationTO) to specific CommodityType.
    private final Map<String, CommodityType>
            commoditySpecMap = Maps.newHashMap();

    private long shoppingListId = 1000L; // Arbitrary start value

    private AtomicLong cloneIndex = new AtomicLong(0);

    private final NumericIDAllocator commodityTypeAllocator = new NumericIDAllocator();

    // used in double comparision
    public static final double EPSILON = 1e-5;

    /**
     * Map from entity OID to original topology entity DTO.
     */
    private Map<Long, TopologyDTO.TopologyEntityDTO> entityOidToDto = Maps.newHashMap();

    private Map<Long, TopologyEntityDTO> unmodifiableEntityOidToDtoMap
            = Collections.unmodifiableMap(entityOidToDto);

    // a map to keep the oid to projected traderTO mapping
    private final Map<Long, EconomyDTOs.TraderTO> oidToProjectedTraderTOMap = Maps.newHashMap();

    // a map to keep the oid to original traderTO mapping
    private final Map<Long, TraderTO> oidToOriginalTraderTOMap = new HashMap<>();

    // Bicliquer created based on datastore
    private final BiCliquer dsBasedBicliquer = new BiCliquer();
    // Bicliquer created based on pm
    private final BiCliquer pmBasedBicliquer = new BiCliquer();

    // Table that stores the number of consumers of a commodity sold by a provider
    private final Table<Long, CommodityType, Integer> numConsumersOfSoldCommTable =
            HashBasedTable.create();
    // Map from bcKey to commodity bought
    private Map<String, CommodityDTOs.CommodityBoughtTO> bcCommodityBoughtMap = Maps.newHashMap();
    // a BiMap from DSPMAccess and Datastore commodity sold key to seller oid
    // Note: the commodity key is composed of entity type and entity ID (which is different from
    // OID)
    private BiMap<String, Long> accessesByKey = HashBiMap.create();

    private final Map<TopologyEntityDTO, TopologyEntityDTO> azToRegionMap = new HashMap<>();

    // This map will hold VM/DB -> BusinessAccount mapping.
    private final Map<TopologyEntityDTO, TopologyEntityDTO> cloudEntityToBusinessAccount = new HashMap<>();
    // This will hold the set of all business accounts in the topology
    private final Set<TopologyEntityDTO> businessAccounts = new HashSet<>();

    private final TopologyInfo topologyInfo;

    private final CloudEntityResizeTracker cert = new CloudEntityResizeTracker();

    private float quoteFactor = AnalysisUtil.QUOTE_FACTOR;
    private float liveMarketMoveCostFactor = AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR;

    // Add a cost of moving from source to destination.
    public static final float PLAN_MOVE_COST_FACTOR = 0.0f;
    public static final float CLOUD_QUOTE_FACTOR = 1;

    private final CommodityConverter commodityConverter;

    private final ActionInterpreter actionInterpreter;

    // A map of the topology entity dto id to its reserved instance coverage
    private final Map<Long, EntityReservedInstanceCoverage> projectedReservedInstanceCoverage = Maps.newHashMap();

    /**
     * Utility to track errors encountered during conversion.
     */
    private final ConversionErrorCounts conversionErrorCounts = new ConversionErrorCounts();

    /**
     * Index that keeps scaling factors applied during conversion TO market entities, to allow
     * quick lookups to reverse scaling when converting FROM market entities.
     */
    private final CommodityIndex commodityIndex;

    /**
     * A non-shop-together TopologyConverter.
     *
     * @param topologyInfo Information about the topology.
     */
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             @Nonnull final MarketPriceTable marketPriceTable,
                             @Nonnull final CloudCostData cloudCostData,
                             @Nonnull final CommodityIndexFactory commodityIndexFactory) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.commodityConverter = new CommodityConverter(commodityTypeAllocator, commoditySpecMap,
                includeGuaranteedBuyer, dsBasedBicliquer, numConsumersOfSoldCommTable, conversionErrorCounts);
        this.cloudTc = new CloudTopologyConverter(unmodifiableEntityOidToDtoMap, topologyInfo,
                pmBasedBicliquer, dsBasedBicliquer, commodityConverter, azToRegionMap, businessAccounts,
                marketPriceTable, cloudCostData);
        this.commodityIndex = commodityIndexFactory.newIndex();
        this.actionInterpreter = new ActionInterpreter(commodityConverter,
            shoppingListOidToInfos,
            cloudTc,
            unmodifiableEntityOidToDtoMap,
            oidToProjectedTraderTOMap,
            cert,
            projectedReservedInstanceCoverage);
    }

    /**
     * Constructor with includeGuaranteedBuyer parameter.
     *
     * @param topologyInfo Information about the topology.
     * @param includeGuaranteedBuyer whether to include guaranteed buyers (VDC, VPod, DPod) or not
     * @param quoteFactor to be used by move recommendations.
     * @param liveMarketMoveCostFactor used by the live market to control aggressiveness of move actions.
     * @param marketPriceTable
     */
    @VisibleForTesting
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             final boolean includeGuaranteedBuyer,
                             final float quoteFactor,
                             final float liveMarketMoveCostFactor,
                             @Nonnull final MarketPriceTable marketPriceTable,
                             @Nonnull final CloudCostData cloudCostData,
                             @Nonnull final CommodityIndexFactory commodityIndexFactory) {
        this(topologyInfo, includeGuaranteedBuyer, quoteFactor, liveMarketMoveCostFactor,
            marketPriceTable, null, cloudCostData, commodityIndexFactory);
    }

    /**
     * Constructor with includeGuaranteedBuyer parameter.
     *
     * @param topologyInfo Information about the topology.
     * @param includeGuaranteedBuyer whether to include guaranteed buyers (VDC, VPod, DPod) or not
     * @param quoteFactor to be used by move recommendations.
     * @param liveMarketMoveCostFactor used by the live market to control aggressiveness of move actions.
     * @param marketPriceTable
     * @param commodityConverter
     * @param cloudCostData
     */
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             final boolean includeGuaranteedBuyer,
                             final float quoteFactor,
                             final float liveMarketMoveCostFactor,
                             @Nonnull final MarketPriceTable marketPriceTable,
                             CommodityConverter commodityConverter,
                             final CloudCostData cloudCostData,
                             final CommodityIndexFactory commodityIndexFactory) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
        this.quoteFactor = quoteFactor;
        this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
        this.commodityConverter = commodityConverter != null ?
                commodityConverter : new CommodityConverter(commodityTypeAllocator, commoditySpecMap,
                    includeGuaranteedBuyer, dsBasedBicliquer, numConsumersOfSoldCommTable, conversionErrorCounts);
        this.cloudTc = new CloudTopologyConverter(unmodifiableEntityOidToDtoMap, topologyInfo,
                pmBasedBicliquer, dsBasedBicliquer, this.commodityConverter, azToRegionMap, businessAccounts,
                marketPriceTable, cloudCostData);
        this.commodityIndex = commodityIndexFactory.newIndex();
        this.actionInterpreter = new ActionInterpreter(this.commodityConverter, shoppingListOidToInfos,
                cloudTc,
            unmodifiableEntityOidToDtoMap,
            oidToProjectedTraderTOMap,
            cert,
            projectedReservedInstanceCoverage);
    }

    @VisibleForTesting
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             final boolean includeGuaranteedBuyer,
                             final float quoteFactor,
                             final float liveMarketMoveCostFactor,
                             @Nonnull final MarketPriceTable marketPriceTable,
                             @Nonnull CommodityConverter commodityConverter,
                             @Nonnull final CommodityIndexFactory commodityIndexFactory) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
        this.quoteFactor = quoteFactor;
        this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
        this.commodityConverter = commodityConverter;
        this.cloudTc = new CloudTopologyConverter(unmodifiableEntityOidToDtoMap, topologyInfo,
                pmBasedBicliquer, dsBasedBicliquer, commodityConverter, azToRegionMap, businessAccounts,
                marketPriceTable, null);
        this.commodityIndex = commodityIndexFactory.newIndex();
        this.actionInterpreter = new ActionInterpreter(commodityConverter, shoppingListOidToInfos,
                cloudTc,
            unmodifiableEntityOidToDtoMap,
            oidToProjectedTraderTOMap,
            cert,
            projectedReservedInstanceCoverage);
    }

    private boolean isPlan() {
        return TopologyDTOUtil.isPlan(topologyInfo);
    }

    /**
     * Convert a collection of common protobuf topology entity DTOs to analysis protobuf economy DTOs.
     * @param topology list of topology entity DTOs
     * @return set of economy DTOs
     */
    @Nonnull
    public Set<EconomyDTOs.TraderTO> convertToMarket(
                @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> topology) {
        // TODO (roman, Jul 5 2018): We don't need to create a new entityOidToDto map.
        // We can have a helper class that will apply the skipped entity logic on the
        // original topology.
        conversionErrorCounts.startPhase(Phase.CONVERT_TO_MARKET);
        try {
            for (TopologyDTO.TopologyEntityDTO entity : topology.values()) {
                int entityType = entity.getEntityType();
                if (AnalysisUtil.SKIPPED_ENTITY_TYPES.contains(entityType)
                    || SKIPPED_ENTITY_STATES.contains(entity.getEntityState())
                    || !includeByType(entityType)) {
                    logger.debug("Skipping trader creation for entity name = {}, entity type = {}, " +
                            "entity state = {}", entity.getDisplayName(),
                        EntityType.forNumber(entity.getEntityType()), entity.getEntityState());
                    skippedEntities.put(entity.getOid(), entity);
                } else {
                    entityOidToDto.put(entity.getOid(), entity);
                    if (isPlan() && CONTAINER_TYPES.contains(entityType)) {
                        // VMs and ContainerPods
                        providersOfContainers.addAll(entity.getCommoditiesBoughtFromProvidersList()
                            .stream()
                            .filter(CommoditiesBoughtFromProvider::hasProviderId)
                            .map(CommoditiesBoughtFromProvider::getProviderId)
                            .collect(Collectors.toSet()));
                    }
                    if (entity.getEntityType() == EntityType.REGION_VALUE) {
                        List<TopologyEntityDTO> AZs = TopologyDTOUtil.getConnectedEntitiesOfType(
                            entity, EntityType.AVAILABILITY_ZONE_VALUE, topology);
                        AZs.forEach(az -> azToRegionMap.put(az, entity));
                    } else if (entity.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE) {
                        List<TopologyEntityDTO> vms = TopologyDTOUtil.getConnectedEntitiesOfType(
                            entity, EntityType.VIRTUAL_MACHINE_VALUE, topology);
                        List<TopologyEntityDTO> dbs = TopologyDTOUtil.getConnectedEntitiesOfType(
                            entity, EntityType.DATABASE_VALUE, topology);
                        List<TopologyEntityDTO> dbss = TopologyDTOUtil.getConnectedEntitiesOfType(
                            entity, EntityType.DATABASE_SERVER_VALUE, topology);
                        vms.forEach(vm -> cloudEntityToBusinessAccount.put(vm, entity));
                        dbs.forEach(db -> cloudEntityToBusinessAccount.put(db, entity));
                        dbss.forEach(db -> cloudEntityToBusinessAccount.put(db, entity));
                        businessAccounts.add(entity);
                    }
                }
            }
            return convertToMarket();
        } finally {
            conversionErrorCounts.endPhase();
        }
    }

    @Nonnull
    public Collection<TopologyEntityDTO> getSkippedEntities() {
        return Collections.unmodifiableCollection(skippedEntities.values());
    }

    /**
     * Convert a map of common protobuf topology entity DTOs to analysis protobuf economy DTOs.
     * @return set of economy DTOs
     */
    @Nonnull
    private Set<EconomyDTOs.TraderTO> convertToMarket() {
        logger.info("Converting topologyEntityDTOs to traderTOs");
        logger.debug("Start creating bicliques");
        BiMap<Long, String> oidToUuidMap = HashBiMap.create();
        for (TopologyDTO.TopologyEntityDTO dto : entityOidToDto.values()) {
            dto.getCommoditySoldListList().stream()
                .filter(comm -> CommodityConverter.isBicliqueCommodity(comm.getCommodityType()))
                .forEach(comm -> edge(dto, comm));
            oidToUuidMap.put(dto.getOid(), String.valueOf(dto.getOid()));
            commodityIndex.addEntity(dto);
            populateCommodityConsumesTable(dto);
        }
        // Create market tier traderTO builders
        List<TraderTO.Builder> marketTierTraderTOBuilders = cloudTc.createMarketTierTraderTOs();
        marketTierTraderTOBuilders.forEach(t -> oidToUuidMap.put(t.getOid(), String.valueOf(t.getOid())));
        dsBasedBicliquer.compute(oidToUuidMap);
        pmBasedBicliquer.compute(oidToUuidMap);
        logger.debug("Done creating bicliques");

        final ImmutableSet.Builder<EconomyDTOs.TraderTO> returnBuilder = ImmutableSet.builder();
        // Convert market tier traderTO builders to traderTOs
        marketTierTraderTOBuilders.stream()
                .map(t -> t.addAllCliques(pmBasedBicliquer.getBcIDs(String.valueOf(t.getOid()))))
                .map(t -> t.addAllCommoditiesSold(commodityConverter.bcCommoditiesSold(t.getOid())))
                .forEach(t -> returnBuilder.add(t.build()));
        entityOidToDto.values().stream()
                .filter(t -> TopologyConversionUtils.shouldConvertToTrader(t.getEntityType()))
                .map(this::topologyDTOtoTraderTO)
                .filter(Objects::nonNull)
                .forEach(returnBuilder::add);
        logger.info("Converted topologyEntityDTOs to traderTOs");
        return returnBuilder.build();
    }

    /**
     * Iterate over all commodities bought by a trader from a supplier and increment the
     * number of consumers associated with the commodities bought in the corresponding seller
     *
     * This information is stored in numConsumersOfSoldCommTable
     *
     */
    private void populateCommodityConsumesTable(TopologyDTO.TopologyEntityDTO dto) {
        // iterate over the commoditiesBought by a buyer on a per seller basis
        dto.getCommoditiesBoughtFromProvidersList().forEach(entry ->
            // for each commodityBought, we increment the number of consumers for the
            // corresponding commSold in numConsumersOfSoldCommTable
            entry.getCommodityBoughtList().forEach(commDto -> {
                Integer consumersCount = numConsumersOfSoldCommTable.get(
                        entry.getProviderId(), commDto.getCommodityType());
                if (consumersCount == null) {
                    consumersCount = 0;
                }
                consumersCount = consumersCount + 1;
                numConsumersOfSoldCommTable.put(entry.getProviderId(),
                        commDto.getCommodityType(),
                        consumersCount);
            })
        );
    }

    private void edge(TopologyDTO.TopologyEntityDTO dto, TopologyDTO.CommoditySoldDTO commSold) {
        accessesByKey.computeIfAbsent(commSold.getCommodityType().getKey(),
            key -> commSold.getAccesses());
        if (commSold.getCommodityType().getType() == CommodityDTO.CommodityType.DSPM_ACCESS_VALUE &&
            dto.getEntityType() == EntityType.STORAGE_VALUE) {
            // Storage id first, PM id second.
            // This way each storage is a member of exactly one biclique.
            String dsOid = String.valueOf(dto.getOid());
            String pmOid = String.valueOf(commSold.getAccesses());
            dsBasedBicliquer.edge(dsOid, pmOid);
        } else if (commSold.getCommodityType().getType() == CommodityDTO.CommodityType.DATASTORE_VALUE &&
            dto.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
            // PM id first, storage id second.
            // This way each pm is a member of exactly one biclique.
            String pmOid = String.valueOf(dto.getOid());
            String dsOid = String.valueOf(commSold.getAccesses());
            pmBasedBicliquer.edge(pmOid, dsOid);
        }
    }

    /**
     * Convert the {@link EconomyDTOs.TraderTO}s to {@link TopologyDTO.ProjectedTopologyEntity}s.
     *
     * @param projectedTraders list of {@link EconomyDTOs.TraderTO}s that are to be converted to
     * {@link TopologyDTO.TopologyEntityDTO}s
     * @param originalTopology the original set of {@link TopologyDTO.TopologyEntityDTO}s by OID.
     * @param priceIndexMessage the price index message
     * @param cloudCostData the cloud cost information
     * @param reservedCapacityAnalysis the reserved capacity information
     * @return list of {@link TopologyDTO.ProjectedTopologyEntity}s
     */
    @Nonnull
    public Map<Long, TopologyDTO.ProjectedTopologyEntity> convertFromMarket(
                @Nonnull final List<EconomyDTOs.TraderTO> projectedTraders,
                @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology,
                @Nonnull final PriceIndexMessage priceIndexMessage,
                @Nonnull final CloudCostData cloudCostData,
                @Nonnull final ReservedCapacityAnalysis reservedCapacityAnalysis) {
        conversionErrorCounts.startPhase(Phase.CONVERT_FROM_MARKET);
        try {
            final Map<Long, PriceIndexMessagePayload> priceIndexByOid =
                priceIndexMessage.getPayloadList().stream()
                    .collect(Collectors.toMap(PriceIndexMessagePayload::getOid, Function.identity()));
            Map<Long, EconomyDTOs.TraderTO> projTraders =
                    projectedTraders.stream().collect(Collectors.toMap(t -> t.getOid(), Function.identity()));
            logger.info("Converting {} projectedTraders to topologyEntityDTOs", projectedTraders.size());
            projectedTraders.forEach(t -> oidToProjectedTraderTOMap.put(t.getOid(), t));
            relinquishCoupons(projectedTraders, cloudCostData);
            final Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedTopologyEntities = new HashMap<>(
                projectedTraders.size());
            for (TraderTO projectedTrader : projectedTraders) {
                final Set<TopologyEntityDTO> projectedEntities =
                    traderTOtoTopologyDTO(projectedTrader, originalTopology, reservedCapacityAnalysis, projTraders);
                for (TopologyEntityDTO projectedEntity : projectedEntities) {
                    final ProjectedTopologyEntity.Builder projectedEntityBuilder =
                        ProjectedTopologyEntity.newBuilder().setEntity(projectedEntity);
                    final PriceIndexMessagePayload priceIndex = priceIndexByOid.get(projectedEntity.getOid());
                    if (priceIndex != null) {
                        if (originalTopology.containsKey(projectedEntity.getOid())) {
                            projectedEntityBuilder.setOriginalPriceIndex(priceIndex.getPriceindexCurrent());
                        }
                        projectedEntityBuilder.setProjectedPriceIndex(priceIndex.getPriceindexProjected());
                    }

                    projectedTopologyEntities.put(projectedEntity.getOid(), projectedEntityBuilder.build());
                }
                // Calculate projected RI Coverage
                TraderTO originalTraderTO = oidToOriginalTraderTOMap.get(projectedTrader.getOid());
                Optional<EntityReservedInstanceCoverage> originalRiCoverage = Optional.empty();
                // original trader can be null in case of provisioned entity
                if (originalTraderTO != null) {
                    originalRiCoverage = cloudCostData
                        .getRiCoverageForEntity(originalTraderTO.getOid());
                }
                calculateProjectedRiCoverage(originalTraderTO, projectedTrader, originalRiCoverage);
            }
            return projectedTopologyEntities;
        } finally {
            conversionErrorCounts.endPhase();
        }
    }

    /**
     * Calculates the projected RI coverage for the given projected trader and adds it to
     * projectedReservedInstanceCoverage.
     * The existing RI Coverage is used if the trader stayed on the same RI and the coverage
     * remained the same.
     * A new projected coverage is created in these cases:
     * 1. If the trader is a new trader (like in add workload plan), and is placed on an RITier.
     * 2. If the trader moved from on demand tier to RI tier.
     * 3. If the trader moved from RI tier 1 to RI tier 2. In this case, use coupons of RI2. Coupons
     * used by trader of RI1 were already relinquished in the relinquishCoupons method.
     * 4. If the trader stayed on same RI, but coverage changed.
     *
     * @param originalTraderTO the original trader (before going into market). Can be null in case
     *                         of initial placement.
     * @param projectedTraderTO the projected trader for which RI Coverage is to be created
     * @param originalRiCoverage the original trader's original RI Coverage (before going into market)
     */
    private void calculateProjectedRiCoverage(TraderTO originalTraderTO, @Nonnull TraderTO projectedTraderTO,
                                              @Nonnull Optional<EntityReservedInstanceCoverage> originalRiCoverage) {
        // Are any of the shopping lists of the projected trader placed on discounted tier?
        Optional<ShoppingListTO> projectedRiTierSl = getShoppingListSuppliedByRiTier(projectedTraderTO);
        if (projectedRiTierSl.isPresent()) {
            // Destination is RI. Get the projected RI Tier and the projected coupon comm bought.
            RiDiscountedMarketTier projectedRiTierDestination = (RiDiscountedMarketTier)
                    cloudTc.getMarketTier(projectedRiTierSl.get().getCouponId());
            Optional<CommodityBoughtTO> projectedCouponCommBought = getCouponCommBought(
                    projectedRiTierSl.get());
            if (!projectedCouponCommBought.isPresent()) {
                logger.error("Projected trader {} is placed on RI {}, but it's RI shopping list does not have " +
                                "projectedCouponCommBought",
                        projectedTraderTO.getDebugInfoNeverUseInCode(),
                        projectedRiTierDestination != null ? projectedRiTierDestination.getDisplayName() : "");
                return;
            }
            // TODO: When we start relinquishing coupons in XL if a VM is resizing, we need to
            // relook at this line below. If we relinquish coupons, the VM's original trader will
            // no longer be supplied by an RIDiscountedMarketTier.
            Optional<MarketTier> originalRiTierDestination = Optional.empty();
            if (originalTraderTO != null) {
                originalRiTierDestination = originalTraderTO.getShoppingListsList().stream()
                        .map(sl -> sl.getSupplier()).map(traderId -> cloudTc.getMarketTier(traderId))
                        .filter(Objects::nonNull).filter(mt -> mt.hasRIDiscount()).findFirst();
            }

            if (!originalRiTierDestination.isPresent() ||
                    !originalRiTierDestination.get().equals(projectedRiTierDestination)) {
                // Entity is initially placed on an riTier
                // OR has moved from a regular tier to an riTier
                // OR from one riTer to another.
                // So, in all these cases, create a new RICoverage object.
                // The information of the number of coupons to use is present in the coupon
                // commodity bought of the trader
                Optional<EntityReservedInstanceCoverage> riCoverage =
                        projectedRiTierDestination.useCoupons(projectedTraderTO.getOid(),
                                projectedCouponCommBought.get().getQuantity());
                riCoverage.ifPresent(coverage -> projectedReservedInstanceCoverage.put(
                        projectedTraderTO.getOid(), coverage));

            } else {
                // Entity stayed on the same RI Tier. Check if the coverage changed.
                // If we are in this block, originalTraderTO cannot be null.
                if (!originalRiCoverage.isPresent()) {
                    logger.error("{} does not have original RI coverage", originalTraderTO.getDebugInfoNeverUseInCode());
                    return;
                }
                float projectedNumberOfCouponsBought = projectedCouponCommBought.get().getQuantity();
                float originalNumberOfCouponsBought = getTotalNumberOfCouponsCovered(originalRiCoverage.get());
                if (!TopologyConversionUtils.areFloatsEqual(
                        projectedCouponCommBought.get().getQuantity(),
                        originalNumberOfCouponsBought)) {
                    // Coverage changed. Create a new RICoverage object
                    Optional<EntityReservedInstanceCoverage> riCoverage =
                            projectedRiTierDestination.useCoupons(projectedTraderTO.getOid(), projectedNumberOfCouponsBought);
                    riCoverage.ifPresent(coverage -> projectedReservedInstanceCoverage.put(projectedTraderTO.getOid(), coverage));
                } else {
                    // Coverage did not change. Use the original ri coverage.
                    projectedReservedInstanceCoverage.put(projectedTraderTO.getOid(), originalRiCoverage.get());
                }
            }
        }
    }

    /**
     * This method goes over all the traders and relinquishes coupons of the
     * RIDiscountedMarketTiers in 3 cases. If a trader moved
     * 1. From RI to On demand. Relinquish the coupons the trader was using of the RI.
     * 2. From RI1 to RI2.  Relinquish the coupons the trader was using of the RI1.
     * 3. From RI1 to RI1 with change in coverage.  Relinquish the original number of coupons the
     * trader was using of the RI1.
     *
     * @param projectedTraders All the projected traders
     * @param cloudCostData Cloud cost data which is used to get original ri coverage
     */
    private void relinquishCoupons(@Nonnull final List<TraderTO> projectedTraders, CloudCostData cloudCostData) {
        for (TraderTO projectedTrader : projectedTraders) {
            TraderTO originalTrader = oidToOriginalTraderTOMap.get(projectedTrader.getOid());
            // Original trader might be null in case of a provisioned trader
            if (originalTrader != null) {
                // If the VM was using an RI before going into market, then original trader will
                // have a shopping list supplied by RIDiscountedMarketTier because in this case
                // while constructing shopping lists of original trader, we make the supplier of
                // the compute shopping list as the trader representing RIDiscountedMarketTier
                Optional<ShoppingListTO> originalRiTierSl = getShoppingListSuppliedByRiTier(originalTrader);
                if (originalRiTierSl.isPresent()) {
                    // Originally trader was placed on RI
                    RiDiscountedMarketTier originalRiTier = (RiDiscountedMarketTier)
                            cloudTc.getMarketTier(originalRiTierSl.get().getSupplier());
                    Optional<EntityReservedInstanceCoverage> originalRiCoverage = cloudCostData
                            .getRiCoverageForEntity(originalTrader.getOid());
                    if (!originalRiCoverage.isPresent()) {
                        logger.error("{} does not have original RI coverage", originalTrader.getDebugInfoNeverUseInCode());
                        return;
                    }
                    Optional<ShoppingListTO> projectedRiTierSl = getShoppingListSuppliedByRiTier(projectedTrader);
                    if (projectedRiTierSl.isPresent()) {
                        if (projectedRiTierSl.get().getSupplier() != originalRiTierSl.get().getSupplier()) {
                            // Entity moved from one RI to another.
                            originalRiTier.relinquishCoupons(originalRiCoverage.get());
                        } else {
                            // Entity stayed on same RI. Did coverage change? If yes relinquish
                            Optional<CommodityBoughtTO> projectedCouponCommBought = getCouponCommBought(projectedRiTierSl.get());
                            if (!projectedCouponCommBought.isPresent()) {
                                // We use the original trader in this error message here because the projected trader does not have debug info
                                logger.error("{} does not have projected coupon commodity bought.", originalTrader.getDebugInfoNeverUseInCode());
                                return;
                            }
                            float originalNumberOfCouponsBought = getTotalNumberOfCouponsCovered(originalRiCoverage.get());
                            if(!TopologyConversionUtils.areFloatsEqual(
                                    projectedCouponCommBought.get().getQuantity(),
                                    originalNumberOfCouponsBought)) {
                                originalRiTier.relinquishCoupons(originalRiCoverage.get());
                            }
                        }
                    } else {
                        // Moved from RI to on demand
                        originalRiTier.relinquishCoupons(originalRiCoverage.get());
                    }
                }
            }
        }
    }

    /**
     * Finds the shopping list of trader which is supplied by a RI Tier. RI Tier is an
     * RiDiscountedMarketTier.
     *
     * @param trader the trader for whose shopping lists will be scanned
     * @return Optional of the ShoppingListTO
     */
    private Optional<ShoppingListTO> getShoppingListSuppliedByRiTier(@Nonnull TraderTO trader) {
        return trader.getShoppingListsList().stream().filter(sl -> sl.hasCouponId()).findFirst();
    }

    /**
     * Finds the Coupon comm bought from a shopping list.
     *
     * @param sl the shopping list whose commodities are scanned for
     * @return Optional of CommodityBoughtTO
     */
    private Optional<CommodityBoughtTO> getCouponCommBought(@Nonnull ShoppingListTO sl) {
        return sl.getCommoditiesBoughtList().stream().filter(c -> commodityConverter
                .economyToTopologyCommodity(c.getSpecification())
                .orElse(CommodityType.getDefaultInstance()).getType() ==
                CommodityDTO.CommodityType.COUPON_VALUE).findFirst();
    }

    /**
     * Gets the total number of coupons covered from a given entity ri coverage.
     *
     * @param riCoverage the entity ri coverage using which total number of coupons covered are calculated.
     * @return the total number of coupons covered
     */
    private float getTotalNumberOfCouponsCovered(@Nonnull EntityReservedInstanceCoverage riCoverage) {
        return (float)riCoverage.getCouponsCoveredByRiMap().values().stream()
                .mapToDouble(Double::new).sum();
    }

    /**
     * Interpret the market-specific {@link ActionTO} as a topology-specific {@Action} proto
     * for use by the rest of the system.
     *
     * It is vital to use the same {@link TopologyConverter} that converted
     * the topology into market-specific objects
     * to interpret the resulting actions in a way that makes sense.
     *
     * @param actionTO An {@link ActionTO} describing an action recommendation
     *                 generated by the market.
     * @param projectedTopology The projected topology. All entities involved in the action are
     *                          expected to be in the projected topology.
     * @param originalCloudTopology {@link CloudTopology} of the original {@link TopologyEntityDTO}s  received by Analysis
     * @param projectedCosts  A map of id of projected topologyEntityDTO -> {@link CostJournal} for the entity with that ID.
     *
     *
     * @return The {@link Action} describing the recommendation in a topology-specific way.
     */
    @Nonnull
    public Optional<Action> interpretAction(@Nonnull final ActionTO actionTO,
                                            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                            @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                            @Nonnull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
                                            @Nonnull TopologyCostCalculator topologyCostCalculator) {
        return actionInterpreter.interpretAction(actionTO, projectedTopology, originalCloudTopology,
                projectedCosts, topologyCostCalculator);
    }

    /**
     * Create CommoditiesBoughtFromProvider for TopologyEntityDTO.
     *
     * @param sl ShoppingListTO of the projectedTraderTO
     * @param commList List of CommodityBoughtDTO
     * @return CommoditiesBoughtFromProvider
     */
    private CommoditiesBoughtFromProvider createCommoditiesBoughtFromProvider(
        EconomyDTOs.ShoppingListTO sl, List<TopologyDTO.CommodityBoughtDTO> commList) {
        final CommoditiesBoughtFromProvider.Builder commoditiesBoughtFromProviderBuilder =
            TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder()
                .addAllCommodityBought(commList);
        // if can not find the ShoppingListInfo, that means Market generate some wrong shopping
        // list.
        ShoppingListInfo slInfo = shoppingListOidToInfos.get(sl.getOid());
        if (slInfo == null) {
            throw new IllegalStateException("Market returned invalid shopping list for : " + sl);
        }
        // Six cases for a sl move:
        // active provider -> active provider/provisioned provider/unplaced
        // unplaced -> active provider/provisioned provider/unplaced
        // skipped provider -> unplaced
        //
        // active provider: provider exists before the market
        // provisioned provider: provider is created by the market
        // unplaced: sl has no provider
        // skipped provider: provider in skippedEntities
        Long supplier = null;
        if (sl.hasSupplier()) {
            supplier = sl.getSupplier();
        } else if (skippedEntities.containsKey(slInfo.sellerId)) {
            // If a sl is unplaced due to skipped provider, which makes it movable false,
            // we should set the supplier of it to the original skipped provider.
            // Why do we need else if here, not just else?
            // Because for a sl moving from active provider to unplaced, the supplier should be null.
            // If we just use else here, then the supplier will be the original active provider,
            // which is not correct.
            supplier = slInfo.sellerId;
        }
        if (supplier != null) {
            // If the supplier is a market tier, then get the tier TopologyEntityDTO and
            // make that the supplier.
            if (cloudTc.isMarketTier(supplier)) {
                supplier = cloudTc.getMarketTier(supplier).getTier().getOid();
            }
            commoditiesBoughtFromProviderBuilder.setProviderId(supplier);
        }
        // For a sl of an unplaced VM before market, it doesn't have a provider, but has
        // providerEntityType. It should remain the same if it's unplaced after market.
        // For a sl moving from active provider/unplaced to provisioned provider,
        // we can get the providerEntityType from slInfo.
        slInfo.getSellerEntityType()
            .ifPresent(commoditiesBoughtFromProviderBuilder::setProviderEntityType);
        slInfo.getResourceId().ifPresent(commoditiesBoughtFromProviderBuilder::setVolumeId);
        return commoditiesBoughtFromProviderBuilder.build();
    }

    /**
     * Convert a {@link EconomyDTOs.TraderTO} to a set of {@link TopologyDTO.TopologyEntityDTO}.
     * Usually one trader will return one topologyEntityDTO. But there are some exceptions.
     * For ex. in the case of cloud VMs, one trader will give back a list of TopologyEntityDTOs
     * which consist of the projected VM and its projected volumes.
     *
     * @param traderTO {@link EconomyDTOs.TraderTO} that is to be converted to a {@link TopologyDTO.TopologyEntityDTO}
     * @param traderOidToEntityDTO whose key is the traderOid and the value is the original
     * traderTO
     * @param reservedCapacityAnalysis the reserved capacity information
     * @return set of {@link TopologyDTO.TopologyEntityDTO}s
     */
    private Set<TopologyDTO.TopologyEntityDTO> traderTOtoTopologyDTO(EconomyDTOs.TraderTO traderTO,
                    @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> traderOidToEntityDTO,
                    @Nonnull final ReservedCapacityAnalysis reservedCapacityAnalysis,
                    @Nonnull final Map<Long, EconomyDTOs.TraderTO> projTraders) {
        Set<TopologyDTO.TopologyEntityDTO> topologyEntityDTOs = Sets.newHashSet();
        if (cloudTc.isMarketTier(traderTO.getOid())) {
            // Tiers and regions are added from the original topology into the projected traders
            // because there can be no change to these entities by market.
            return topologyEntityDTOs;
        }
        List<TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider> topoDTOCommonBoughtGrouping =
            new ArrayList<>();
        long pmOid = 0L;
        List<Long> storageOidList = Collections.emptyList();
        // for a VM, find its associated PM to ST list map
        if (traderTO.getType() == EntityType.VIRTUAL_MACHINE_VALUE) {
            Map<Long, List<Long>> pm2stMap = createPMToSTMap(traderTO.getShoppingListsList());
            // there should be only one pm supplier for vm, zero if the vm is unplaced
            if (!pm2stMap.isEmpty()) {
                pmOid = pm2stMap.keySet().iterator().next();
                storageOidList = pm2stMap.get(pmOid);
            }
        }
        TopologyDTO.TopologyEntityDTO originalEntity = traderOidToEntityDTO.get(traderTO.getOid());
        for (EconomyDTOs.ShoppingListTO sl : traderTO.getShoppingListsList()) {
            List<TopologyDTO.CommodityBoughtDTO> commList = new ArrayList<>();
            int bicliqueBaseType = commodityTypeAllocator.allocate(
                    TopologyConversionConstants.BICLIQUE);
            for (CommodityDTOs.CommodityBoughtTO commBought : sl.getCommoditiesBoughtList()) {
                // if the commodity bought DTO type is biclique, create new
                // DSPM and Datastore bought
                if (bicliqueBaseType == commBought.getSpecification().getBaseType()) {
                    if (pmOid == sl.getSupplier()) {
                        // create datastore commodity bought because the supplier is PM
                        for (Long stOid: storageOidList) {
                            // iterate all storage supplier, create all datastore commodity bought
                            // for vm-pm shoppinglist
                            commList.add(newCommodity(CommodityDTO.CommodityType.DATASTORE_VALUE, stOid));
                        }

                    } else if (storageOidList.contains(sl.getSupplier())) {
                        // create dspm commodity bought because the supplier is ST
                        commList.add(newCommodity(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE, pmOid));
                    }
                } else {
                    commBoughtTOtoCommBoughtDTO(traderTO.getOid(), sl.getSupplier(), sl.getOid(),
                        commBought, reservedCapacityAnalysis).ifPresent(commList::add);
                }
            }
            // the shopping list might not exist in shoppingListOidToInfos, because it might be
            // created inside M2 via a provision by demand or provision by supply action
            if (shoppingListOidToInfos.get(sl.getOid()) == null) {
                TraderTO supplier = projTraders.get(sl.getSupplier());
                logger.trace("Adding shopping list {} of trader {} having a supplier {} into the sl-info map",
                                sl.getOid(), traderTO.getDebugInfoNeverUseInCode(),
                                supplier != null ? supplier.getDebugInfoNeverUseInCode() : null);
                ShoppingListInfo slInfo = new ShoppingListInfo(sl.getOid(), traderTO.getOid(),
                                sl.getSupplier(), null,
                                supplier != null ? supplier.getType() : null, commList);
                shoppingListOidToInfos.put(sl.getOid(), slInfo);
            }
            topoDTOCommonBoughtGrouping.add(createCommoditiesBoughtFromProvider(sl, commList));
        }

        TopologyDTO.EntityState entityState = TopologyDTO.EntityState.POWERED_ON;
        String displayName = originalEntity != null ? originalEntity.getDisplayName()
                        : traderOidToEntityDTO.get(traderTO.getCloneOf()).getDisplayName()
                        + "_Clone #" + cloneIndex.addAndGet(1);
        if (originalEntity != null) {
            EntityState originalState = originalEntity.getEntityState();
            // TODO: Address the following workaroud for SUSPEND VM.
            // Without the workaround for VM:
            // Entity (SUSPEND) -> TRADER (IDLE) -> TRADER (ACTIVE) -> Entity (POWER_ON)
            // The correct behavior for VM:
            // Entity (SUSPEND) -> TRADER (IDLE) -> TRADER (IDLE) -> Entity (SUSPEND)
            // For VMs, if trader states are not changed, then the new entity state
            // should be the same as original entity state.
            if (traderTO.getType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                EconomyDTOs.TraderTO oldTraderTo = topologyDTOtoTraderTO(originalEntity);
                if (oldTraderTo != null
                        && isSameVMTraderState(traderTO.getState(), oldTraderTo.getState())) {
                    entityState = originalEntity.getEntityState();
                }
            } else {
                // set state of IDLE VM to poweredOff
                entityState = originalState == TopologyDTO.EntityState.POWERED_OFF
                        ? TopologyDTO.EntityState.POWERED_OFF
                        : TopologyDTO.EntityState.POWERED_ON;
            }
        }
        if (entityState == TopologyDTO.EntityState.POWERED_ON) {
            entityState = (traderTO.getState() == EconomyDTOs.TraderStateTO.ACTIVE)
                            ? TopologyDTO.EntityState.POWERED_ON
                            : TopologyDTO.EntityState.SUSPENDED;
        }

        final TraderSettingsTO traderSetting = traderTO.getSettings();
        TopologyDTO.TopologyEntityDTO.AnalysisSettings analysisSetting =
            TopologyDTO.TopologyEntityDTO.AnalysisSettings.newBuilder()
                .setIsAvailableAsProvider(traderSetting.getCanAcceptNewCustomers())
                .setShopTogether(traderSetting.getIsShopTogether())
                .setCloneable(traderSetting.getClonable())
                .setSuspendable(traderSetting.getSuspendable())
                .setDesiredUtilizationTarget((traderSetting.getMaxDesiredUtilization()
                       + traderSetting.getMinDesiredUtilization()) / 2)
                .setDesiredUtilizationRange(traderSetting.getMaxDesiredUtilization()
                       - traderSetting.getMinDesiredUtilization())
                .setProviderMustClone(traderSetting.getProviderMustClone())
            .build();
        TopologyDTO.TopologyEntityDTO.Builder entityDTOBuilder =
                TopologyDTO.TopologyEntityDTO.newBuilder()
                    .setEntityType(traderTO.getType())
                    .setOid(traderTO.getOid())
                    .setEntityState(entityState)
                    .setDisplayName(displayName)
                    .addAllCommoditySoldList(retrieveCommSoldList(traderTO))
                    .addAllCommoditiesBoughtFromProviders(topoDTOCommonBoughtGrouping)
                    .addAllConnectedEntityList(getConnectedEntities(traderTO))
                    .setAnalysisSettings(analysisSetting);
        if (originalEntity == null) {
            // this is a clone trader
            originalEntity = traderOidToEntityDTO.get(traderTO.getCloneOf());
            entityDTOBuilder.setOrigin(Origin.newBuilder()
                .setAnalysisOrigin(AnalysisOrigin.newBuilder()
                    .setOriginalEntityId(originalEntity.getOid())));
        } else {
            // copy the origin from the original entity
            if (originalEntity.hasOrigin()) {
                entityDTOBuilder.setOrigin(originalEntity.getOrigin());
            }
        }

        copyStaticAttributes(originalEntity, entityDTOBuilder);

        // get dspm and datastore commodity sold from the original trader, add
        // them to projected topology entity DTO
        TopologyEntityDTO entityDTO = entityDTOBuilder.addAllCommoditySoldList(
                originalEntity.getCommoditySoldListList().stream()
                        .filter(c -> AnalysisUtil.DSPM_OR_DATASTORE
                                .contains(c.getCommodityType().getType()))
                        .collect(Collectors.toSet()))
                .build();
        topologyEntityDTOs.add(entityDTO);
        topologyEntityDTOs.addAll(createResources(entityDTO));
        return topologyEntityDTOs;
    }

    /**
     * Copies static attributes from one TopologyEntityDTO to another.
     * Static attributes will not change between the original entity to the projected entity.
     *
     * @param source the source TopologyEntityDto to copy from
     * @param destination the destination TopologyEntityDto.Builder to copy to
     */
    private void copyStaticAttributes(TopologyEntityDTO source, TopologyEntityDTO.Builder destination) {
        // copy environmentType
        if (source.hasEnvironmentType()) {
            destination.setEnvironmentType(source.getEnvironmentType());
        }

        // copy the TypeSpecificInfo from the original entity
        if (source.hasTypeSpecificInfo()) {
            destination.setTypeSpecificInfo(source.getTypeSpecificInfo());
        }
    }

    /**
     * Create entities for resources of topologyEntityDTO.
     * For ex. If a Cloud VM has a volume, then we create the projected version of the volume here.
     *
     * @param topologyEntityDTO The entity for which the resources need to be created
     * @return set of resources
     */
    @VisibleForTesting
    Set<TopologyEntityDTO> createResources(TopologyEntityDTO topologyEntityDTO) {
        Set<TopologyEntityDTO> resources = Sets.newHashSet();
        for(CommoditiesBoughtFromProvider commBoughtGrouping :
                topologyEntityDTO.getCommoditiesBoughtFromProvidersList()) {
            // create entities for volumes
            if (commBoughtGrouping.hasVolumeId()) {
                TopologyEntityDTO originalVolume = entityOidToDto.get(commBoughtGrouping.getVolumeId());
                if (originalVolume != null && originalVolume.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    if (!commBoughtGrouping.hasProviderId() || !commBoughtGrouping.hasProviderEntityType()) {
                        logger.error("commBoughtGrouping of projected entity {} has volume Id {} " +
                            "but no associated storage or storageTier",
                            topologyEntityDTO.getDisplayName(), commBoughtGrouping.getVolumeId());
                        continue;
                    }
                    // Build a volume which is connected to the same Storage or StorageTier
                    // (which is the provider for this commBoughtGrouping), and connected to
                    // the same AZ as the VM if the zone exists
                    TopologyEntityDTO.Builder volume =
                        TopologyEntityDTO.newBuilder()
                            .setEntityType(originalVolume.getEntityType())
                            .setOid(originalVolume.getOid());

                    // connect to storage or storage tier
                    ConnectedEntity connectedStorageOrStorageTier = ConnectedEntity.newBuilder()
                            .setConnectedEntityId(commBoughtGrouping.getProviderId())
                            .setConnectedEntityType(commBoughtGrouping.getProviderEntityType())
                            .setConnectionType(ConnectionType.NORMAL_CONNECTION).build();
                    volume.addConnectedEntityList(connectedStorageOrStorageTier);

                    // Get the AZ or Region the VM is connected to (there is no zone for azure or on-prem)
                    List<TopologyEntityDTO> azOrRegion = TopologyDTOUtil.getConnectedEntitiesOfType(
                        topologyEntityDTO,
                        Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE),
                        entityOidToDto);
                    if (!azOrRegion.isEmpty()) {
                        // Use the first AZ or Region we get.
                        ConnectedEntity connectedAzOrRegion = ConnectedEntity.newBuilder()
                            .setConnectedEntityId(azOrRegion.get(0).getOid())
                            .setConnectedEntityType(azOrRegion.get(0).getEntityType()).build();
                        volume.addConnectedEntityList(connectedAzOrRegion);
                    }
                    copyStaticAttributes(originalVolume, volume);
                    volume.setDisplayName(originalVolume.getDisplayName());
                    resources.add(volume.build());
                }
            }
        }
        return resources;
    }

    /**
     * Gets the list of connected entities to a trader. At present, this method returns the
     * availability zone to be connected to the trader for cloud consumers.
     *
     * @param traderTO the traderTO for which connected entity list is desired
     * @return
     */
    @Nonnull
    private List<ConnectedEntity> getConnectedEntities(TraderTO traderTO) {
        List<ConnectedEntity> connectedEntities = new ArrayList<>();
        TopologyEntityDTO originalCloudConsumer = entityOidToDto.get(traderTO.getOid());
        // Copy the connected entities of original entity into the projected entity except for the
        // ones in the map, which need to be computed like Availability zone or Region
        // because the AZ or Region might have changed.
        if (originalCloudConsumer != null) {
            Set<Integer> connectionsToCompute = projectedConnectedEntityTypesToCompute
                    .get(originalCloudConsumer.getEntityType());
            originalCloudConsumer.getConnectedEntityListList().stream()
                    .filter(ce -> connectionsToCompute == null ||
                            !connectionsToCompute.contains(ce.getConnectedEntityType()))
                    .forEach(ce -> connectedEntities.add(ConnectedEntity.newBuilder()
                            .setConnectedEntityId(ce.getConnectedEntityId())
                            .setConnectedEntityType(ce.getConnectedEntityType())
                            .setConnectionType(ce.getConnectionType()).build()));
            if (cloudTc.isTraderConsumingFromMaketTier(traderTO)) {
                // Primary market tier refers to Compute market tier / database market tier
                MarketTier destinationPrimaryMarketTier = cloudTc.getPrimaryMarketTier(traderTO);
                if (destinationPrimaryMarketTier == null) {
                    logger.error("Could not fetch primary market tier for {}",
                            traderTO.getDebugInfoNeverUseInCode());
                } else {
                    TopologyEntityDTO sourceRegion = cloudTc.getRegionOfCloudConsumer(originalCloudConsumer);
                    TopologyEntityDTO destinationRegion = destinationPrimaryMarketTier.getRegion();
                    TopologyEntityDTO destAZOrRegion = null;
                    if (sourceRegion == destinationRegion) {
                        // cloud consumer (VM / DB) did NOT move to a different region
                        destAZOrRegion = cloudTc.getAZOfCloudConsumer(originalCloudConsumer);
                    } else {
                        // cloud consumer (VM / DB) has moved to a different region
                        // Pick the first AZ in the destination region if AZ exists
                        List<TopologyEntityDTO> destAZs = TopologyDTOUtil.getConnectedEntitiesOfType(destinationRegion,
                                EntityType.AVAILABILITY_ZONE_VALUE, unmodifiableEntityOidToDtoMap);
                        if (!destAZs.isEmpty()) {
                            destAZOrRegion = destAZs.get(0);
                        }
                    }
                    destAZOrRegion = destAZOrRegion != null ? destAZOrRegion : destinationRegion;
                    ConnectedEntity az = ConnectedEntity.newBuilder()
                        .setConnectedEntityId(destAZOrRegion.getOid())
                        .setConnectedEntityType(destAZOrRegion.getEntityType())
                        .setConnectionType(ConnectionType.NORMAL_CONNECTION).build();
                    connectedEntities.add(az);
                }
            }
        }
        return connectedEntities;
    }

    /**
     * For VM, IDLE trader can be ACTIVE after analysis. It's a workaround to treat ACTIVE as IDEL,
     * and vise verse.
     * @param newState new VM state
     * @param originalState original VM state
     * @return true if the they have "same" state.
     */
    private boolean isSameVMTraderState(@Nonnull final TraderStateTO newState,
                                        @Nonnull final TraderStateTO originalState) {
        Objects.requireNonNull(newState);
        Objects.requireNonNull(originalState);
        if (originalState == TraderStateTO.ACTIVE && newState == TraderStateTO.IDLE) {
            return true;
        }
        if (originalState == TraderStateTO.IDLE && newState == TraderStateTO.ACTIVE) {
            return true;
        }
        return originalState == newState;
    }

    private TopologyDTO.CommodityBoughtDTO newCommodity(int type, long pmOid) {
        final CommodityType.Builder typeBuilder =
                    CommodityType.newBuilder().setType(type);

        final String key = accessesByKey.inverse().get(pmOid);
        if (key != null) {
            typeBuilder.setKey(key);
        } else {
            logger.warn("Unable to find key in inverse map for PM with OID {}", pmOid);
        }

        return TopologyDTO.CommodityBoughtDTO.newBuilder()
            .setCommodityType(typeBuilder)
            .build();
    }

    /**
     * Create a map which contains a VM's PM supplier to all ST suppliers mapping.
     *
     * @param shoppingListList all shoppinglists of the given trader
     * @return a map in which key is a PM OID and the value is a list of ST Oids that the
     * PM is connected to
     */
    private Map<Long, List<Long>> createPMToSTMap(
                    List<EconomyDTOs.ShoppingListTO> shoppingListList) {
        Map<Long, List<Long>> pMToSTMap = Maps.newHashMap();
        // return an empty map if there is no trader in oidToProjectedTraderTOMap
        if (oidToProjectedTraderTOMap.isEmpty()) {
            return pMToSTMap;
        }
        List<Long> stList = new ArrayList<>();
        for (EconomyDTOs.ShoppingListTO sl : shoppingListList) {
            if (oidToProjectedTraderTOMap.containsKey(sl.getSupplier())) {
                long supplierType = oidToProjectedTraderTOMap.get(sl.getSupplier()).getType();
                if (supplierType == EntityType.PHYSICAL_MACHINE_VALUE) {
                    pMToSTMap.put(sl.getSupplier(), stList);
                } else if (supplierType == EntityType.STORAGE_VALUE) {
                    stList.add(sl.getSupplier());
                }
            }
        }
        return pMToSTMap;
    }

    /**
     * Convert commodities sold by a trader to a list of {@link TopologyDTO.CommoditySoldDTO}.
     *
     * @param traderTO {@link TraderTO} whose commoditiesSold are to be converted into DTOs
     * @return list of {@link TopologyDTO.CommoditySoldDTO}s
     */
    private Set<TopologyDTO.CommoditySoldDTO> retrieveCommSoldList(
            @Nonnull final TraderTO traderTO) {
        return traderTO.getCommoditiesSoldList().stream()
                    .map(commoditySoldTO -> commSoldTOtoCommSoldDTO(traderTO.getOid(), commoditySoldTO))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toSet());
    }

    /**
     * Gets the resized capacity of cloud entity based on the used, weighted used and resize target
     * utilization.
     *
     * @param topologyDTO topology entity DTO as resize target
     * @param commBought commodity bought to be resized
     * @param providerOid the oid of the seller of the shopping list
     * @return an array of two elements, the first element is new used value,
     * the second is the new peak value
     */
    protected double[] getResizedCapacityForCloud(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO,
            @Nonnull final TopologyDTO.CommodityBoughtDTO commBought,
            @Nullable final Long providerOid) {

        final float histUsed =
                CommodityConverter.getUsedValue(commBought, TopologyDTO.CommodityBoughtDTO::getUsed,
                        commBought.hasHistoricalUsed() ?
                                TopologyDTO.CommodityBoughtDTO::getHistoricalUsed : null);
        final float histPeak =
                CommodityConverter.getUsedValue(commBought, TopologyDTO.CommodityBoughtDTO::getPeak,
                        commBought.hasHistoricalPeak() ?
                                TopologyDTO.CommodityBoughtDTO::getHistoricalPeak : null);

        // TODO: Need to add check for Cloud migration here. This will apply to Cloud Migration too.
        if (topologyDTO.getEnvironmentType() != EnvironmentType.CLOUD) {
            return new double[]{histUsed, histPeak};
        }

        final Integer drivingCommSoldType =
                TopologyConversionConstants.commDependancyMapForCloudResize.get(
                        commBought.getCommodityType().getType());

        if (drivingCommSoldType != null) {
            final List<TopologyDTO.CommoditySoldDTO> drivingCommmoditySoldList =
                    topologyDTO.getCommoditySoldListList()
                            .stream()
                            .filter(c -> c.getCommodityType().getType() == drivingCommSoldType)
                            .collect(Collectors.toList());
            if (!drivingCommmoditySoldList.isEmpty()) {
                final CommoditySoldDTO commoditySoldDTO = drivingCommmoditySoldList.get(0);
                final double[] resizedQuantity =
                        calculateResizedQuantity(commoditySoldDTO.getUsed(),
                                getWeightedUsed(commoditySoldDTO), commoditySoldDTO.getUsed(),
                                commoditySoldDTO.getPeak(), commoditySoldDTO.getCapacity(),
                                commoditySoldDTO.getResizeTargetUtilization());
                cert.logCommodityResize(topologyDTO.getOid(), commoditySoldDTO.getCommodityType(),
                        resizedQuantity[0] - commoditySoldDTO.getCapacity());
                logger.debug(
                        "Using a peak used of {} for driving commodity type {} for entity {}.",
                        resizedQuantity[1], commoditySoldDTO.getCommodityType().getType(),
                        topologyDTO.getDisplayName());
                return resizedQuantity;
            }
        } else if (providerOid != null &&
                TopologyConversionConstants.THROUGHPUT_COMMODITIES.contains(
                        commBought.getCommodityType().getType())) {
            final MarketTier marketTier = cloudTc.getMarketTier(providerOid);
            if (marketTier != null) {
                final TopologyEntityDTO tier = marketTier.getTier();
                final Optional<CommoditySoldDTO> commoditySoldDTO = tier
                        .getCommoditySoldListList()
                        .stream()
                        .filter(c -> c.getCommodityType().equals(commBought.getCommodityType()))
                        .filter(CommoditySoldDTO::hasCapacity)
                        .findFirst();
                if (commoditySoldDTO.isPresent()) {
                    // We want to use the historical used (already smoothened) for both the resize-up
                    // and resize-down demand calculations. We do not want to consider the
                    // historical max or the peaks to avoid a one-time historic max value to cause
                    // resize decisions.
                    final double[] resizedQuantity = calculateResizedQuantity(histUsed, histUsed,
                            commBought.getUsed(), histPeak,
                            commoditySoldDTO.get().getCapacity(),
                            commBought.getResizeTargetUtilization());
                    cert.logCommodityResize(topologyDTO.getOid(), commBought.getCommodityType(),
                            resizedQuantity[0] - commoditySoldDTO.get().getCapacity());
                    logger.debug("Using a peak used of {} for commodity type {} for entity {}.",
                            resizedQuantity[1], commBought.getCommodityType().getType(),
                            topologyDTO.getDisplayName());
                    return resizedQuantity;
                } else {
                    logger.debug("Tier {} does not sell commodity type {} for entity {}",
                            tier::getDisplayName, commBought::getCommodityType,
                            topologyDTO::getDisplayName);
                }
            }
        }

        return new double[]{histUsed, histPeak};
    }

    private double[] calculateResizedQuantity(double resizeUpDemand, double resizeDownDemand,
            double used, double peak, double capacity, double targetUtil) {
        if (targetUtil <= 0.0) {
            targetUtil = EntitySettingSpecs.UtilTarget.getSettingSpec()
                    .getNumericSettingValueType()
                    .getDefault() / 100D;
        }

        // For cloud entities / cloud migration plans, we do not use the peakQuantity.
        // We only use the quantity values inside M2
        final double peakQuantity = Math.max(used, peak) / targetUtil;
        final double quantity;
        if (Math.ceil(resizeUpDemand / targetUtil) > capacity) {
            quantity = resizeUpDemand / targetUtil;
        } else if (resizeDownDemand > 0 && Math.ceil(resizeDownDemand / targetUtil) < capacity) {
            quantity = resizeDownDemand / targetUtil;
        } else {
            // do not resize
            quantity = capacity;
        }
        return new double[]{quantity, peakQuantity};
    }

    /**
     * Calculates the weighted usage which will be used for resize down for cloud resource.
     *
     * @param commodity the {@link TopologyDTO.CommoditySoldDTO}
     */
    private double getWeightedUsed(@Nonnull final TopologyDTO.CommoditySoldDTO commodity) {
        final Double maxQuantity =
                commodity.hasHistoricalUsed() && commodity.getHistoricalUsed().hasMaxQuantity() ?
                        commodity.getHistoricalUsed().getMaxQuantity() : null;
        final float max = maxQuantity == null ? 0f : maxQuantity.floatValue();
        return getWeightedUsed(commodity.getUsed(), max, commodity.getPeak());
    }


    /**
     * Calculates the weighted usage which will be used for resize down for cloud resource.
     *
     * @param used commodity used
     * @param max max quantity of commodity historical used
     * @param peak commodity peak
     */
    private static double getWeightedUsed(double used, double max, double peak) {
        return max <= 0 ? max : TopologyConversionConstants.RESIZE_AVG_WEIGHT * used +
                TopologyConversionConstants.RESIZE_MAX_WEIGHT * max +
                TopologyConversionConstants.RESIZE_PEAK_WEIGHT * peak;
    }

    /**
     * Convert {@link CommodityDTOs.CommodityBoughtTO} of a trader to its corresponding
     * {@link TopologyDTO.CommodityBoughtDTO}.
     * <p/>
     * Note that the 'used' value may need to be reverse-scaled from the market-value to the
     * 'topology' value if there is a 'scaleFactor' for that commodity (e.g. CPU, CPU_Provisioned).
     *
     * @param commBoughtTO {@link CommodityBoughtTO} that is to be converted to
     * {@link TopologyDTO.CommodityBoughtDTO}
     * @param reservedCapacityAnalysis the reserved capacity information
     * @return {@link TopologyDTO.CommoditySoldDTO} that the trader sells
     */
    @Nonnull
    private Optional<TopologyDTO.CommodityBoughtDTO> commBoughtTOtoCommBoughtDTO(
        final long traderOid, final long supplierOid, final long slOid,
        @Nonnull final CommodityBoughtTO commBoughtTO,
        @Nonnull final ReservedCapacityAnalysis reservedCapacityAnalysis) {

        float peak = commBoughtTO.getPeakQuantity();
        if (peak < 0) {
            conversionErrorCounts.recordError(ErrorCategory.PEAK_NEGATIVE,
                commodityConverter.getCommodityName(commBoughtTO.getSpecification().getType()));
            logger.trace("The entity with negative peak is {} (buying {} from {})",
                traderOid, commBoughtTO, supplierOid);
            peak = 0;
        }

        final float peakQuantity = peak; // It must be final

        long volumeId = shoppingListOidToInfos.get(slOid) != null && shoppingListOidToInfos.get(slOid).getResourceId().isPresent() ?
                shoppingListOidToInfos.get(slOid).getResourceId().get() : 0;
        return commodityConverter.economyToTopologyCommodity(commBoughtTO.getSpecification())
                .map(commType -> TopologyDTO.CommodityBoughtDTO.newBuilder()
                    .setUsed(reverseScaleCommBought(commBoughtTO.getQuantity(),
                        commodityIndex.getCommBought(traderOid, supplierOid, commType, volumeId)))
                    .setReservedCapacity(reservedCapacityAnalysis.getReservedCapacity(traderOid, commType))
                    .setCommodityType(commType)
                    .setPeak(peakQuantity)
                    .build());
    }

    private EconomyDTOs.TraderTO topologyDTOtoTraderTO(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO) {
        EconomyDTOs.TraderTO traderDTO = null;
        try {
            final EconomyDTOs.TraderStateTO state = TopologyConversionUtils.traderState(topologyDTO);
            final boolean active = EconomyDTOs.TraderStateTO.ACTIVE.equals(state);
            final boolean bottomOfSupplyChain = topologyDTO.getCommoditiesBoughtFromProvidersList().isEmpty();
            final boolean topOfSupplyChain = topologyDTO.getCommoditySoldListList().isEmpty();
            final int entityType = topologyDTO.getEntityType();
            boolean clonable = EntitySettings.BooleanKey.ENABLE_PROVISION.value(topologyDTO);
            /*
             * Whether trader is suspendable in market or not depends on multiple conditions
             * 1. Topology settings - policy sends suspendable flag as false
             * (entity settings and analysis settings (happens later))
             * 2. Topology sends controllable flag as false
             * 3. Whether entity is a VM in plan (to improve)
             * 4. Whether in plan the VM hosts containers (in which case trump 3)
             * 5. Whether entity is top of supply chain (to improve)
             * 6. Whether it is a DB or DBServer on cloud
             */
            boolean suspendable = EntitySettings.BooleanKey.ENABLE_SUSPEND.value(topologyDTO);
            boolean isProviderMustClone = EntitySettings.BooleanKey
                    .PROVIDER_MUST_CLONE.value(topologyDTO);
            if (bottomOfSupplyChain && active) {
                suspendable = false;
            }
            if (isPlan() && entityType == EntityType.VIRTUAL_MACHINE_VALUE) {
                suspendable = false;
            }
            // Checking isPlan here is redundant, but since Set.contains(.) might be expensive,
            // (even for an empty Set) it is worth checking again. Also improves readability.
            if (isPlan() && providersOfContainers.contains(topologyDTO.getOid())) {
                clonable = true;
                suspendable = true;
            }
            if (topOfSupplyChain) { // Workaround for OM-25254. Should be set by mediation.
                suspendable = false;
            }
            // If topologyEntity set clonable value, we should directly use it.
            clonable = topologyDTO.getAnalysisSettings().hasCloneable()
                            ? topologyDTO.getAnalysisSettings().getCloneable()
                                            : clonable;
            // If topologyEntity set suspendable value, we should directly use it.
            suspendable = topologyDTO.getAnalysisSettings().hasSuspendable()
                            ? topologyDTO.getAnalysisSettings().getSuspendable()
                                            : suspendable;
            // If topologyEntity set providerMustClone, we need to use its value.
            isProviderMustClone = topologyDTO.getAnalysisSettings().hasProviderMustClone()
                ? topologyDTO.getAnalysisSettings().getProviderMustClone()
                : isProviderMustClone;

            if (entityType == EntityType.DATABASE_VALUE ||
                    entityType == EntityType.DATABASE_SERVER_VALUE) {
                suspendable = false;
            }

            final StitchingErrors stitchingErrors = StitchingErrors.fromProtobuf(topologyDTO);
            final boolean controllable = topologyDTO.getAnalysisSettings().getControllable() &&
                // If there were stitching errors, it's risky to control this entity.
                stitchingErrors.isNone();

            boolean isEntityFromCloud = TopologyConversionUtils.isEntityConsumingCloud(topologyDTO);
            TraderSettingsTO.Builder settingsBuilder = TopologyConversionUtils.
                    createCommonTraderSettingsTOBuilder(topologyDTO, unmodifiableEntityOidToDtoMap);
            settingsBuilder.setClonable(clonable && controllable)
                    .setControllable(controllable)
                    // cloud providers do not come here. We will hence be setting this to true just for
                    // on-prem storages
                    .setCanSimulateAction(entityType == EntityType.STORAGE_VALUE)
                    .setSuspendable(suspendable)
                    .setCanAcceptNewCustomers(topologyDTO.getAnalysisSettings().getIsAvailableAsProvider()
                                              && controllable)
                    .setIsEligibleForResizeDown(isPlan() ||
                            topologyDTO.getAnalysisSettings().getIsEligibleForResizeDown())
                    .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                            .setSumOfCommodity(SumOfCommodity.getDefaultInstance()))
                    .setQuoteFactor(isEntityFromCloud ? CLOUD_QUOTE_FACTOR :quoteFactor)
                    .setMoveCostFactor((isPlan() || isEntityFromCloud)
                            ? PLAN_MOVE_COST_FACTOR
                            : liveMarketMoveCostFactor)
                    .setProviderMustClone(isProviderMustClone);
            if (cloudEntityToBusinessAccount.get(topologyDTO) != null) {
                settingsBuilder.setBalanceAccount(createBalanceAccountDTO(topologyDTO));
            }
            final EconomyDTOs.TraderSettingsTO settings = settingsBuilder.build();

            //compute biclique IDs for this entity, the clique list will be used only for
            // shop together placement, so pmBasedBicliquer is called
            Set<Long> allCliques = pmBasedBicliquer.getBcIDs(String.valueOf(topologyDTO.getOid()));

            traderDTO = EconomyDTOs.TraderTO.newBuilder()
                    // Type and Oid are the same in the topology DTOs and economy DTOs
                    .setOid(topologyDTO.getOid())
                    .setType(entityType)
                    .setState(state)
                    .setSettings(settings)
                    .setDebugInfoNeverUseInCode(entityDebugInfo(topologyDTO))
                    .addAllCommoditiesSold(createAllCommoditySoldTO(topologyDTO))
                    .addAllShoppingLists(createAllShoppingLists(topologyDTO))
                    .addAllCliques(allCliques)
                    .build();
            oidToOriginalTraderTOMap.put(traderDTO.getOid(), traderDTO);
        } catch (Exception e) {
            logger.error(entityDebugInfo(topologyDTO) + " could not be converted to traderTO:", e);
        }
        return traderDTO;
    }

    @Nullable
    private BalanceAccountDTO createBalanceAccountDTO(TopologyEntityDTO topologyEntityDTO) {
        TopologyEntityDTO businessAccount = cloudEntityToBusinessAccount.get(topologyEntityDTO);
        if (businessAccount == null) {
            return null;
        }
        // We create default balance account
        double defaultBudgetValue = 100000000d;
        float spent = 0f;
        return BalanceAccountDTO.newBuilder().setBudget(defaultBudgetValue).setSpent(spent)
                .setId(businessAccount.getOid()).build();
    }

    private @Nonnull List<CommoditySoldTO> createAllCommoditySoldTO(@Nonnull TopologyEntityDTO topologyDTO) {
        final boolean shopTogether = topologyDTO.getAnalysisSettings().getShopTogether();
        List<CommoditySoldTO> commSoldTOList = new ArrayList<>();
        commSoldTOList.addAll(commodityConverter.commoditiesSoldList(topologyDTO));
        if (!shopTogether) {
            commSoldTOList.addAll(commodityConverter.bcCommoditiesSold(topologyDTO.getOid()));
        }
        return commSoldTOList;
    }

    /**
     * Construct a string that can be used for debug purposes.
     *
     * The debug info format should be: EntityType|OID|DisplayName
     * do not change the format, otherwise the analysis stats collection will break
     * This format is the same in both classic and XL
     * TODO this should be enforced in a better way
     *
     * @param topologyDTO the topology entity DTO
     * @return a string in the format "VIRTUAL_MACHINE|1234|VM-1" where "VM-1" is the entity
     * display name.
     */
    @Nonnull
    private static String entityDebugInfo(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO) {
        return EntityType.forNumber(topologyDTO.getEntityType())
                + "|"
                + topologyDTO.getOid()
                + "|"
                + topologyDTO.getDisplayName();
    }

    private static final String EMPTY_JSON = "{}";

    /**
     * Create the shopping lists for a topology entity. A shopping list is
     * a collection of commodities bought from the same provider.
     *
     * @param topologyEntity a topology entity received from the topology processor
     * @return list of shopping lists bought by the corresponding trader
     */
    @Nonnull
    private List<EconomyDTOs.ShoppingListTO> createAllShoppingLists(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyEntity) {
        // used for the case when a plan VM is unplaced
        Map<Long, Long> providers = oldProviders(topologyEntity);
        return topologyEntity.getCommoditiesBoughtFromProvidersList().stream()
                // skip converting shoppinglist that buys from VDC
                // TODO: we also skip the sl that consumes AZ which contains Zone commodity because zonal RI is not yet supported
                .filter(commBoughtGrouping -> includeByType(commBoughtGrouping.getProviderEntityType())
                        && commBoughtGrouping.getProviderEntityType() != EntityType.AVAILABILITY_ZONE_VALUE)
                .map(commBoughtGrouping -> createShoppingList(
                        topologyEntity,
                        topologyEntity.getEntityType(),
                        topologyEntity.getAnalysisSettings().getShopTogether(),
                        getProviderId(commBoughtGrouping, topologyEntity),
                        commBoughtGrouping,
                        providers))
                .collect(Collectors.toList());
    }

    /**
     * Entity type is included in the converted topology if {@link #includeGuaranteedBuyer} is
     * true (in which case it is included regardless of its actual type), or else if the type
     * is not in the list of {@link AnalysisUtil#GUARANTEED_BUYER_TYPES}. This method is used
     * to decide whether to include an entity in the topology by its type, and whether to
     * include a shopping list by its provider type.
     *
     * @param entityType the type of entity to consider for inclusion in the converted topology
     * @return whether to include this entity type in the converted topology
     */
    private boolean includeByType(int entityType) {
        return includeGuaranteedBuyer
            || !AnalysisUtil.GUARANTEED_BUYER_TYPES.contains(entityType);
    }

    /**
     * Extract the old providers mapping. When creating a copy of an entity in
     * a plan, we "un-place" the entity by changing the provider oids in the
     * cloned shopping lists to oids that do not exist in the topology. But we
     * still need to know who were the original providers (for the purpose of
     * creating bicliques). This map provides the mapping between the provider
     * oids in the cloned object and the provider oids in the source object.
     *
     * @param topologyEntity a buyer in the topology
     * @return map from provider oids in the cloned shopping list and
     * the source shopping lists. If the entity is not a clone in a plan
     * then the map is empty.
     */
    private Map<Long, Long> oldProviders(TopologyEntityDTO topologyEntity) {
        // TODO: OM-26631 - get rid of unstructured data and Gson
        @SuppressWarnings("unchecked")
        Map<String, Double> oldProviders = new Gson().fromJson(
            topologyEntity.getEntityPropertyMapMap()
                .getOrDefault("oldProviders", EMPTY_JSON), Map.class);
        return oldProviders.entrySet().stream()
            .collect(Collectors.toMap(e -> Long.decode(e.getKey()),
                e -> e.getValue().longValue()));
    }

    /**
     * Create a shopping list for a specified buyer and the entity it is buying from.
     *
     * @param buyer the buyer of the shopping list
     * @param entityType the entity type of the buyer
     * @param shopTogether whether the entity supports the shop-together feature
     * @param providerOid the oid of the seller of the shopping list
     * @param commBoughtGrouping the commodities bought group by the buyer from the provider
     * @param providers a map that captures the previous placement for unplaced plan entities
     * @return a shopping list between the buyer and seller
     */
    @Nonnull
    private EconomyDTOs.ShoppingListTO createShoppingList(
            final TopologyDTO.TopologyEntityDTO buyer,
            final int entityType,
            final boolean shopTogether,
            @Nullable final Long providerOid,
            @Nonnull final CommoditiesBoughtFromProvider commBoughtGrouping,
            final Map<Long, Long> providers) {
        long buyerOid = buyer.getOid();
        TopologyDTO.TopologyEntityDTO provider = (providerOid != null) ? entityOidToDto.get(providerOid) : null;
        float moveCost = !isPlan() && (entityType == EntityType.VIRTUAL_MACHINE_VALUE
                && provider != null // this check is for testing purposes
                && provider.getEntityType() == EntityType.STORAGE_VALUE)
                        ? (float)(totalStorageAmountBought(buyer) / Units.KIBI)
                        : 0.0f;
        Set<CommodityDTOs.CommodityBoughtTO> values = commBoughtGrouping.getCommodityBoughtList()
            .stream()
            .filter(CommodityBoughtDTO::getActive)
            .map(topoCommBought -> convertCommodityBought(buyer, topoCommBought, providerOid,
                    shopTogether, providers))
            .filter(Objects::nonNull) // Null for DSPMAccess/Datastore and shop-together
            .collect(Collectors.toSet());
        if (cloudTc.isMarketTier(providerOid)) {
            // For cloud buyers, add biClique comm bought because we skip them in
            // convertCommodityBought method
            if (!shopTogether) {
                createBcCommodityBoughtForCloudEntity(providerOid, buyerOid).forEach(values::add);
            }
            // Create DC comm bought
            createDCCommodityBoughtForCloudEntity(providerOid, buyerOid).ifPresent(values::add);
            // Create Coupon Comm
            createCouponCommodityBoughtForCloudEntity(providerOid, buyerOid).ifPresent(values::add);
        }
        final long id = shoppingListId++;
        // Check if the provider of the shopping list is UNKNOWN. If true, set movable false.
        final boolean isProviderUnknown = skippedEntities.containsKey(providerOid) &&
            skippedEntities.get(providerOid).getEntityState() == TopologyDTO.EntityState.UNKNOWN;
        if (isProviderUnknown) {
            logger.debug("Making movable false for shoppingList of entity {} which has provider {} in UNKNOWN state",
                buyer.getDisplayName(), skippedEntities.get(providerOid).getDisplayName());
        }
        final boolean isMovable = !isProviderUnknown && (commBoughtGrouping.hasMovable()
            ? commBoughtGrouping.getMovable()
            : AnalysisUtil.MOVABLE_TYPES.contains(entityType));
        // if the buyer of the shopping list is in control state(controllable = false), or if the
        // shopping list has a provider and the provider is in control state (controllable = false)
        // the shopping list should not move
        final boolean isControllable = entityOidToDto.get(buyerOid).getAnalysisSettings()
                        .getControllable() && (provider == null || (provider != null &&
                        provider.getAnalysisSettings().getControllable()));
        final EconomyDTOs.ShoppingListTO.Builder economyShoppingListBuilder = EconomyDTOs.ShoppingListTO
                .newBuilder()
                .addAllCommoditiesBought(values)
                .setOid(id)
                .setStorageMoveCost(moveCost)
                .setMovable(isMovable && isControllable);
        if (providerOid != null) {
            economyShoppingListBuilder.setSupplier(providerOid);
        }
        final Integer providerEntityType = commBoughtGrouping.hasProviderEntityType() ?
                commBoughtGrouping.getProviderEntityType() : null;
        final Long resourceId = commBoughtGrouping.hasVolumeId() ? commBoughtGrouping.getVolumeId() : null;
        shoppingListOidToInfos.put(id,
            new ShoppingListInfo(id, buyerOid, providerOid, resourceId, providerEntityType,
                    commBoughtGrouping.getCommodityBoughtList()));
        return economyShoppingListBuilder.build();
    }

    /**
     * Creates a DC Comm bought for a cloud entity which has a provider as a Compute tier,
     * DatabaseTier or DatabaseServerTier.
     *
     * @param providerOid oid of the market tier provider oid
     * @param buyerOid oid of the buyer of the shopping list
     * @return The commodity bought TO
     */
    private Optional<CommodityBoughtTO> createDCCommodityBoughtForCloudEntity(
            long providerOid, long buyerOid) {
        MarketTier marketTier = cloudTc.getMarketTier(providerOid);
        int providerEntityType = marketTier.getTier().getEntityType();
        CommodityBoughtTO dcCommBought = null;
        if (CLOUD_ENTITY_TYPES_TO_CREATE_DC_COMM_BOUGHT.contains(providerEntityType)) {
            TopologyEntityDTO region = cloudTc.getRegionOfCloudConsumer(entityOidToDto.get(buyerOid));
            List<CommoditySoldDTO> dcCommSoldList = region.getCommoditySoldListList().stream()
                .filter(commSold -> commSold.getCommodityType().getType() ==
                    CommodityDTO.CommodityType.DATACENTER_VALUE)
                .filter(commSold -> commSold.getCommodityType().hasKey())
                .collect(Collectors.toList());
            if (dcCommSoldList.size() != 1) {
                logger.error("{} is selling {} DC Commodities - {}", region.getDisplayName(),
                        dcCommSoldList.size(), dcCommSoldList.stream().map(
                                c -> c.getCommodityType().getKey())
                                .collect(Collectors.joining(",")));
                return Optional.empty();
            }
            CommoditySoldDTO dcCommSold = dcCommSoldList.iterator().next();
            dcCommBought = CommodityDTOs.CommodityBoughtTO.newBuilder()
                    .setSpecification(commodityConverter.commoditySpecification(
                            dcCommSold.getCommodityType()))
                    .build();
        }
        return Optional.ofNullable(dcCommBought);
    }

    /**
     * Creates a Coupon Comm bought for a cloud entity which has a provider as a Compute tier.
     *
     * @param providerOid oid of the market tier provider oid
     * @param buyerOid oid of the buyer of the shopping list
     * @return The coupon commodity bought TO
     */
    private Optional<CommodityBoughtTO> createCouponCommodityBoughtForCloudEntity(
            long providerOid, long buyerOid) {
        MarketTier marketTier = cloudTc.getMarketTier(providerOid);
        int providerEntityType = marketTier.getTier().getEntityType();
        CommodityBoughtTO couponCommBought = null;
        if (providerEntityType == EntityType.COMPUTE_TIER_VALUE) {
            Optional<EntityReservedInstanceCoverage> riCoverage = cloudTc.getRiCoverageForEntity(buyerOid);
            float couponQuantity = 0;
            if (riCoverage.isPresent()) {
                couponQuantity = getTotalNumberOfCouponsCovered(riCoverage.get());
            }
            couponCommBought = CommodityBoughtTO.newBuilder()
                    .setSpecification(commodityConverter.commoditySpecification(
                            CommodityType.newBuilder()
                                    .setType(CommodityDTO.CommodityType.COUPON_VALUE)
                                    .build()))
                    .setQuantity(couponQuantity).build();
        }
        return Optional.ofNullable(couponCommBought);
    }

    /**
     * Creates the BiClique commodity bought if the provider is a compute market tier
     * or a storage market tier
     *
     * @param providerOid oid of the market tier provider oid
     * @param buyerOid oid of the buyer of the shopping list
     * @return The set of biclique commodity bought TO
     */
    @Nonnull
    private Set<CommodityDTOs.CommodityBoughtTO> createBcCommodityBoughtForCloudEntity(
            long providerOid, long buyerOid) {
        MarketTier marketTier = cloudTc.getMarketTier(providerOid);
        int providerEntityType = marketTier.getTier().getEntityType();
        TopologyEntityDTO cloudBuyer = entityOidToDto.get(buyerOid);
        Set<String> bcKeys = new HashSet<>();
        if (providerEntityType == EntityType.COMPUTE_TIER_VALUE) {
            Set<Long> connectedStorageMarketTierOids =
                    cloudTc.getMarketTierProviderOidOfType(cloudBuyer, EntityType.STORAGE_TIER_VALUE);
            connectedStorageMarketTierOids.stream().filter(Objects::nonNull)
                    .map(stOid -> dsBasedBicliquer.getBcKey(
                            String.valueOf(providerOid), String.valueOf(stOid)))
                    .filter(Objects::nonNull)
                    .forEach(bcKeys::add);
        } else if (providerEntityType == EntityType.STORAGE_TIER_VALUE) {
            dsBasedBicliquer.getBcKeys(String.valueOf(providerOid))
                    .stream().filter(Objects::nonNull)
                    .forEach(bcKeys::add);
        }
        return bcKeys.stream().map(this::bcCommodityBought)
                .filter(Objects::nonNull).collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * The total used value of storage amount commodities bought by a buyer,
     * summed over all the providers that it buys storage amount from.
     *
     * @param buyer the {@link TopologyEntityDTO} of the buyer (presumably a VM)
     * @return total used storage amount bought
     */
    private double totalStorageAmountBought(@Nonnull final TopologyEntityDTO buyer) {
        final MutableDouble result = new MutableDouble(0);
        buyer.getCommoditiesBoughtFromProvidersList().forEach(commsBoughtFromProvider -> {
            if (isStorage(commsBoughtFromProvider)) {
                commsBoughtFromProvider.getCommodityBoughtList().forEach(commodity -> {
                    if (commodity.getCommodityType().getType() ==
                            CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE) {
                        result.add(commodity.getUsed());
                    }
                });
            }
        });
        return result.doubleValue();
    }

    /**
     * Checks whether the provider id of commodity bought group is the OID of a Storage.
     *
     * @param grouping a group of Commodity bought
     * @return whether the provider id is the oid of a Storage
     */

    private boolean isStorage(CommoditiesBoughtFromProvider grouping) {
        if (!grouping.hasProviderId()) {
            return false;
        }
        TopologyDTO.TopologyEntityDTO entity = entityOidToDto.get(grouping.getProviderId());
        return entity != null && entity.getEntityType() == EntityType.STORAGE_VALUE;
    }

    @Nullable
    private Long getProviderId(@Nonnull CommoditiesBoughtFromProvider commodityBoughtGrouping,
                               @Nonnull TopologyEntityDTO topologyEntity) {
        Long providerId = commodityBoughtGrouping.hasProviderId() ?
            commodityBoughtGrouping.getProviderId() :
            null;
        if (providerId == null) {
            return null;
        }
        // If the provider id is a compute tier / storage tier/ database tier, then it is a
        // cloud VM / DB. In this case, get the marketTier from
        // [tier x region] combination.
        TopologyEntityDTO providerTopologyEntity = entityOidToDto.get(providerId);
        if (providerTopologyEntity != null && TopologyDTOUtil.isTierEntityType(
                providerTopologyEntity.getEntityType())) {
            // Provider is a compute tier / storage tier / database tier
            // Get the region connected to the topologyEntity
            Optional<EntityReservedInstanceCoverage> coverage = cloudTc
                    .getRiCoverageForEntity(topologyEntity.getOid());
            TopologyEntityDTO region = cloudTc.getRegionOfCloudConsumer(topologyEntity);
            if (providerTopologyEntity.getEntityType() == EntityType.COMPUTE_TIER_VALUE &&
                    coverage.isPresent()) {
                long riId = coverage.get().getCouponsCoveredByRiMap().keySet().iterator().next();
                ReservedInstanceData riData = cloudTc.getRiDataById(riId);
                ReservedInstanceSpecInfo spec = riData.getReservedInstanceSpec().getReservedInstanceSpecInfo();
                providerId = cloudTc.getTraderTOOid(new RiDiscountedMarketTier(entityOidToDto.get(spec.getTierId()),
                        region, riData, entityOidToDto));
            } else {
                providerId = cloudTc.getTraderTOOid(new OnDemandMarketTier(
                        providerTopologyEntity, region));
            }
        }
        return providerId;
    }

    private CommodityDTOs.CommodityBoughtTO convertCommodityBought(
            final TopologyDTO.TopologyEntityDTO buyer, @Nonnull final TopologyDTO.CommodityBoughtDTO topologyCommBought,
            @Nullable final Long providerOid,
            final boolean shopTogether,
            final Map<Long, Long> providers) {
        CommodityType type = topologyCommBought.getCommodityType();
        return CommodityConverter.isBicliqueCommodity(type)
            ? shopTogether || cloudTc.isMarketTier(providerOid)
                // skip DSPMAcess and Datastore commodities in the case of shop-together
                // or cloud provider
                ? null
                // convert them to biclique commodities if not shop-together
                : generateBcCommodityBoughtTO(
                    providers.getOrDefault(providerOid, providerOid), type)
            // all other commodities - convert to DTO regardless of shop-together
            : createAndValidateCommBoughtTO(buyer, topologyCommBought, providerOid);
    }

    private CommodityDTOs.CommodityBoughtTO createAndValidateCommBoughtTO(
            final TopologyDTO.TopologyEntityDTO buyer,
            TopologyDTO.CommodityBoughtDTO topologyCommBought, @Nullable final Long providerOid) {
        final double[] newQuantity = getResizedCapacityForCloud(buyer, topologyCommBought, providerOid);
        double usedQuantity = newQuantity[0];
        double peakQuantity = newQuantity[1];

        if (usedQuantity < 0) {
            // We don't want to log every time we get used = -1 because mediation
            // sets some values to -1 as default.
            if (logger.isDebugEnabled() || usedQuantity != -1) {
                logger.info("Setting negative used value for "
                                + topologyCommBought.getCommodityType() + " to 0.");
            }
            usedQuantity = 0;
        }
        // if a scalingFactor is specified, scale this commodity up on the way into the market
        if (topologyCommBought.hasScalingFactor()) {
            usedQuantity *= topologyCommBought.getScalingFactor();
        }

        if (peakQuantity < 0) {
            // We don't want to log every time we get peak = -1 because mediation
            // sets some values to -1 as default.
            if (logger.isDebugEnabled() || peakQuantity != -1) {
                logger.info("Setting negative peak value for "
                                + topologyCommBought.getCommodityType() + " to 0.");
            }
            peakQuantity = 0;
        }
        return CommodityDTOs.CommodityBoughtTO.newBuilder()
                .setQuantity((float)usedQuantity)
                .setPeakQuantity((float)peakQuantity)
                .setSpecification(commodityConverter.commoditySpecification(
                        topologyCommBought.getCommodityType()))
                .build();
    }

    /**
     * Generate Biclique commodity bought based on providerOid parameter. And if can not generate
     * Biclique commodity, it will return null. And all those null will be filtered out at createShoppingList
     * function.
     *
     * @param providerOid Oid of provider, it could be null.
     * @param type Commodity Bought type.
     * @return Biclique Commodity bought.
     */
    @Nullable
    private CommodityDTOs.CommodityBoughtTO generateBcCommodityBoughtTO(@Nullable final Long providerOid,
                                                                      final CommodityType type) {
        if (providerOid == null) {
            // TODO: After we remove provider ids of commodity bought for clone entities of Plan,
            // then we need to refactor TopologyConverter class to allow this case.
            logger.error("Biclique commodity bought type {} doesn't have provider id");
            return null;
        }
        // Get the biclique ID and use it to create biclique commodity bought for shop alone
        // entities
        final Optional<String> bcKey = getBcKeyWithProvider(providerOid, type);
        return bcKey
                .map(this::bcCommodityBought)
                .orElse(null);
    }

    /**
     * Obtain the biclique ID from bicliquer that is created based on storage.
     * @return a list of Strings that will be used as biclique commodity keys.
     */
    @Nonnull
    private Optional<String> getBcKeyWithProvider(Long providerOid, CommodityType type) {
        return Optional.ofNullable(dsBasedBicliquer.getBcKey(
                String.valueOf(providerOid),
                String.valueOf(accessesByKey.get(type.getKey()))));
    }

    @Nonnull
    private CommodityDTOs.CommodityBoughtTO bcCommodityBought(@Nonnull String bcKey) {
        return bcCommodityBoughtMap.computeIfAbsent(bcKey,
            key -> CommodityDTOs.CommodityBoughtTO.newBuilder()
                .setSpecification(commodityConverter.bcSpec(key))
                .build());
    }

    /**
     * Convert a single {@link CommoditySoldTO} market dto to the corresponding
     * {@link CommoditySoldDTO} XL model object.
     * <p/>
     * Note that if the original CommoditySoldDTO has a 'scaleFactor', then
     * we reverse the scaling that had been done on the way in to the market.
     *
     * @param traderOid The ID of the trader selling the commodity.
     * @param commSoldTO the market CommdditySoldTO to convert
     * @return a {@link CommoditySoldDTO} equivalent to the original.
     */
    @Nonnull
    private Optional<TopologyDTO.CommoditySoldDTO> commSoldTOtoCommSoldDTO(
            final long traderOid,
            @Nonnull final CommoditySoldTO commSoldTO) {

        float peak = commSoldTO.getPeakQuantity();
        if (peak < 0) {
            conversionErrorCounts.recordError(ErrorCategory.PEAK_NEGATIVE,
                commodityConverter.getCommodityName(commSoldTO.getSpecification().getType()));
            logger.trace("The entity with negative peak is {}", traderOid);
            peak = 0;
        }

        float peakQuantity = peak;

        float capacity = commSoldTO.getCapacity();
        // Get the cpuCoreMhz of the PM if the current commodityType is VCPU.
        if (commSoldTO.getSpecification().getBaseType() == CommodityDTO.CommodityType.VCPU_VALUE) {
            capacity = calculateVCPUResizeCapacityForVM(traderOid, commSoldTO);
        }

        Optional<CommodityType> commTypeOptional =
            commodityConverter.economyToTopologyCommodity(commSoldTO.getSpecification());
        if (!commTypeOptional.isPresent()) {
            return Optional.empty();
        }
        CommodityType commType = commTypeOptional.get();
        // find original sold commodity of same type from original entity
        Optional<CommoditySoldDTO> originalCommoditySold =
            commodityIndex.getCommSold(traderOid, commType);

        CommoditySoldDTO.Builder commoditySoldBuilder = CommoditySoldDTO.newBuilder()
            .setCapacity(reverseScaleCommSold(capacity, originalCommoditySold))
            .setUsed(reverseScaleCommSold(commSoldTO.getQuantity(), originalCommoditySold))
            .setPeak(peakQuantity)
            .setIsResizeable(commSoldTO.getSettings().getResizable())
            .setEffectiveCapacityPercentage(
                commSoldTO.getSettings().getUtilizationUpperBound() * 100)
            .setCommodityType(commType)
            .setIsThin(commSoldTO.getThin())
            .setCapacityIncrement(
                commSoldTO.getSettings().getCapacityIncrement());

        // set hot add / hot remove, if present
        originalCommoditySold
            .filter(CommoditySoldDTO::hasHotResizeInfo)
            .map(CommoditySoldDTO::getHotResizeInfo)
            .ifPresent(commoditySoldBuilder::setHotResizeInfo);

        return Optional.of(commoditySoldBuilder.build());
    }

    /**
     * Calculate the correct VCPU capacity only for VM. We don't need to consider Container.
     * For example, if the old capacity of the VCPU is 5200 MHz, the new capacity calculated by the
     * market is 7000 MHz and the CPU Core MHz of the PM which the current trader stays on is 2600 MHz,
     * 7000 MHz is not the correct VCPU capacity because 7000 MHz is not a multiplier of 2600, which
     * doesn't make sense to resize the VCPU to 7000 / 2600 = 2.7 cores.
     * So we need to round 7000 MHz up to the next multiplier of 2600 larger than 7000, which is 7800.
     * Math formula: correctCapacity = Math.ceil(newCapacity / cpuCoreMhz) * cpuCoreMhz
     *
     * @param traderOid The ID of the trader selling the commodity
     * @param commSoldTO the market CommdditySoldTO to convert
     * @return the correct VCPU capacity
     */
    private float calculateVCPUResizeCapacityForVM(final long traderOid,
                                                   @Nonnull final CommoditySoldTO commSoldTO) {
        float capacity = commSoldTO.getCapacity();
        if (oidToProjectedTraderTOMap.get(traderOid).getType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            return capacity;
        }
        // A VM may be cloned from another VM.
        long originalTraderOid = oidToProjectedTraderTOMap.get(traderOid).hasCloneOf() ?
            oidToProjectedTraderTOMap.get(traderOid).getCloneOf() : traderOid;
        CommoditySoldTO originalCommSoldTO = oidToOriginalTraderTOMap.get(originalTraderOid)
            .getCommoditiesSoldList().stream().filter(commoditySoldTO ->
                commoditySoldTO.getSpecification().getBaseType() == CommodityDTO.CommodityType.VCPU_VALUE)
            .findFirst().get();
        // Check if VCPU is resized.
        int isVCPUresized = Float.compare(capacity, originalCommSoldTO.getCapacity());
        if (isVCPUresized == 0) {
            return capacity;
        }
        // Get the id of the current PM provider of the current trader.
        Optional<Long> providerIdOptional = oidToProjectedTraderTOMap.get(traderOid)
            .getShoppingListsList().stream().filter(ShoppingListTO::hasSupplier)
            .map(ShoppingListTO::getSupplier).filter(supplier ->
                oidToProjectedTraderTOMap.get(supplier).getType() == EntityType.PHYSICAL_MACHINE_VALUE)
            .findFirst();
        if (providerIdOptional.isPresent() &&
            oidToProjectedTraderTOMap.containsKey(providerIdOptional.get())) {
            // Get the id of the original PM provider of the current trader.
            long providerId = oidToProjectedTraderTOMap.get(providerIdOptional.get()).hasCloneOf() ?
                oidToProjectedTraderTOMap.get(providerIdOptional.get()).getCloneOf() :
                providerIdOptional.get();
            boolean hasCpuCoreMhz = entityOidToDto.get(providerId).hasTypeSpecificInfo() &&
                entityOidToDto.get(providerId).getTypeSpecificInfo().hasPhysicalMachine() &&
                entityOidToDto.get(providerId).getTypeSpecificInfo().getPhysicalMachine().hasCpuCoreMhz();
            if (hasCpuCoreMhz) {
                // Always take the ceiling.
                // Same as what we do in ActionTranslator#translateVcpuResizeInfo
                int cpuCoreMhz = entityOidToDto.get(providerId).getTypeSpecificInfo()
                    .getPhysicalMachine().getCpuCoreMhz();
                capacity = (float) Math.ceil(capacity / cpuCoreMhz) * cpuCoreMhz;
            } else {
                logger.error("PM {} doesn't have cpuCoreMhz information.",
                    entityOidToDto.get(providerId).getDisplayName());
            }
        } else {
            logger.error("VM {} has no PM provider.",
                entityOidToDto.get(traderOid).getDisplayName());
        }
        return capacity;
    }

    /**
     * If this commodity-sold in the original {@link TopologyEntityDTO} in the input topology
     * had a scale factor, then reverse the scaling on the way out. In other words, since
     * we multiplied by the scale factor on the way in to the market analysis, here we
     * divide by the scale factor on the way back out.
     * <p/>
     * Note that if there is a scale factor but that scale factor is zero, which should never happen,
     * we simply return the originalValue, i.e. we do _not_ return Inf or NaN.
     *
     * @param valueToReverseScale the commodity value output from the market to
     *                            scale down (if there is a scaleFactor)
     * @return either the valueToReverseScale divided by the scaleFactor if the original
     * commodity had defined a scaleFactor, else the valueToReverseScale unmodified
     */
    private double reverseScaleCommSold(final float valueToReverseScale,
                                        @Nonnull final Optional<CommoditySoldDTO> originalCommSold) {
        if (originalCommSold.isPresent()) {
            // If it's unset, it will be 0.
            double scalingFactor = originalCommSold.get().getScalingFactor();
            // Scaling factor should be positive, and not an infinitely small value.
            if (scalingFactor == 1.0 || (scalingFactor - EPSILON) <= 0) {
                return valueToReverseScale;
            } else {
                return valueToReverseScale / scalingFactor;
            }
        } else {
            return valueToReverseScale;
        }
    }

    /**
     * If this commodity-bought in the original {@link TopologyEntityDTO} in the input topology
     * had a scale factor, then reverse the scaling on the way out. In other words, since
     * we multiplied by the scale factor on the way in to the market analysis, here we
     * divide by the scale factor on the way back out.
     * <p/>
     * Note that if there is a scale factor but that scale factor is zero, which should never happen,
     * we simply return the originalValue, i.e. we do _not_ return Inf or NaN.
     *
     * @param valueToReverseScale the commodity value output from the market to
     *                            scale down (if there is a scaleFactor)
     * @param originalCommBought The original {@link CommodityBoughtDTO} if any.
     * @return either the valueToReverseScale divided by the scaleFactor if the original
     * commodity had defined a scaleFactor, else the valueToReverseScale unmodified
     */
    private double reverseScaleCommBought(final float valueToReverseScale,
                                          @Nonnull final Optional<CommodityBoughtDTO> originalCommBought) {
        // Using if/else instead of .map() to avoid auto-boxing.
        if (originalCommBought.isPresent()) {
            // If unset, it will be 0.
            double scalingFactor = originalCommBought.get().getScalingFactor();
            // Scaling factor should be positive, and not an infinitely small value.
            if (scalingFactor == 1.0 || (scalingFactor - EPSILON) <= 0) {
                return valueToReverseScale;
            } else {
                return valueToReverseScale / scalingFactor;
            }
        } else {
            return valueToReverseScale;
        }
    }

    @VisibleForTesting
    CommodityConverter getCommodityConverter() {
        return commodityConverter;
    }

    /**
     * Adds the newly cloned entity's shoppinglist to shoppingListInfos.
     *
     * @param list the list of shoppinglist to its buyer mapping
     */
    public void updateShoppingListMap(List<NewShoppingListToBuyerEntry> list) {
        // economy only sends back information about shoppinglist oid and its buyer oid,
        // action interpretation only cares about shoppinglist oid and buyer oid,
        // however, because the definition of ShoppingListInfo requires providerOid and
        // commodityBoughtList, we have to give some dummy values
        list.forEach(l -> shoppingListOidToInfos.put(l.getNewShoppingList(),
                    new ShoppingListInfo(l.getNewShoppingList(), l.getBuyer(), null, null, null,
                            Lists.newArrayList())));
    }

    /**
     * Gets the unmodifiable version of projected RI coverage.
     *
     * @return A map of entity id to its projected reserved instance coverage
     */
    public Map<Long, EntityReservedInstanceCoverage> getProjectedReservedInstanceCoverage() {
        return Collections.unmodifiableMap(projectedReservedInstanceCoverage);
    }
}
