package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
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

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.Pair;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.market.runner.ReservedCapacityAnalysis;
import com.vmturbo.market.runner.WastedFilesAnalysis;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.settings.EntitySettings;
import com.vmturbo.market.settings.MarketSettings;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.RiDiscountedMarketTier;
import com.vmturbo.market.topology.SingleRegionMarketTier;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.market.topology.conversions.CommodityIndex.CommodityIndexFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ScalingGroup;
import com.vmturbo.market.topology.conversions.ConversionErrorCounts.ErrorCategory;
import com.vmturbo.market.topology.conversions.ConversionErrorCounts.Phase;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults.NewShoppingListToBuyerEntry;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CoverageEntry;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessagePayload;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.SumOfCommodity;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
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

    // If a cloud entity buys from this set of cloud entity types, then we need to create DataCenter
    // commodity bought for it.
    private static final Set<Integer> CLOUD_ENTITY_TYPES_TO_CREATE_DC_COMM_BOUGHT = ImmutableSet.of(
        EntityType.COMPUTE_TIER_VALUE,
        EntityType.DATABASE_TIER_VALUE,
        EntityType.DATABASE_SERVER_TIER_VALUE
    );

    private static final Logger logger = LogManager.getLogger();

    // TODO: In legacy this is taken from LicenseManager and is currently false
    private boolean includeGuaranteedBuyer = INCLUDE_GUARANTEED_BUYER_DEFAULT;

    @VisibleForTesting
    CloudTopologyConverter cloudTc;

    /**
     * Entities that are providers of containers.
     * Populated only for plans. For realtime market, this set will be empty.
     */
    private Set<Long> providersOfContainers = Sets.newHashSet();

    // Set of oids of compute shopping list of the cloud vm. one entry per cloud vm.
    private Set<Long> cloudVmComputeShoppingListIDs = new HashSet<>();

    // Store skipped service entities which need to be added back to projected topology and price
    // index messages.
    private Map<Long, TopologyEntityDTO> skippedEntities = Maps.newHashMap();

    @VisibleForTesting
    protected Map<Long, TopologyEntityDTO> getSkippedEntities() {
        return skippedEntities;
    }

    // a map keeps shoppinglist oid to ShoppingListInfo which is a container for
    // shoppinglist oid, buyer oid, seller oid and commodity bought
    private final Map<Long, ShoppingListInfo> shoppingListOidToInfos = Maps.newHashMap();

    public Set<Long> getCloudVmComputeShoppingListIDs() {
        return cloudVmComputeShoppingListIDs;
    }

    /**
     * A non-shop-together TopologyConverter.
     *
     * @param topologyInfo information about topology
     * @param marketPriceTable market price table
     * @param cloudCostData cloud cost data
     * @param commodityIndexFactory commodity index factory
     * @param tierExcluderFactory tier excluder factory
     * @param consistentScalingHelperFactory CSM helper factory
     */
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             @Nonnull final MarketPriceTable marketPriceTable,
                             @Nonnull final CloudCostData cloudCostData,
                             @Nonnull final CommodityIndexFactory commodityIndexFactory,
                             @NonNull final TierExcluderFactory tierExcluderFactory,
                             @Nonnull final ConsistentScalingHelperFactory
                                                consistentScalingHelperFactory) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.cloudTopology = null;
        this.consistentScalingHelper = consistentScalingHelperFactory
            .newConsistentScalingHelper(topologyInfo, getShoppingListOidToInfos());
        this.commodityConverter = new CommodityConverter(commodityTypeAllocator, commoditySpecMap,
                includeGuaranteedBuyer, dsBasedBicliquer, numConsumersOfSoldCommTable,
                conversionErrorCounts, consistentScalingHelper);
        this.tierExcluder = tierExcluderFactory.newExcluder(topologyInfo, this.commodityConverter,
            getShoppingListOidToInfos());
        this.cloudTc = new CloudTopologyConverter(unmodifiableEntityOidToDtoMap, topologyInfo,
                pmBasedBicliquer, dsBasedBicliquer, commodityConverter, azToRegionMap, businessAccounts,
                marketPriceTable, cloudCostData, tierExcluder, cloudTopology);
        this.commodityIndex = commodityIndexFactory.newIndex();
        this.enableSMA = false;
        this.actionInterpreter = new ActionInterpreter(commodityConverter,
            shoppingListOidToInfos,
            cloudTc,
            unmodifiableEntityOidToDtoMap,
            oidToProjectedTraderTOMap,
            cert,
            projectedReservedInstanceCoverage, tierExcluder);
    }

    /**
     * get the TopologyEntityDTO OID corresponding to the oid of a TemplateProvider.
     * @param traderTOOID  oid of a TemplateProvider
     * @return the OID of corresponding TopologyEntityDTO
     */
    public Long convertTraderTOToTopologyEntityDTO(Long traderTOOID) {
        return cloudTc.getMarketTier(traderTOOID).getTier().getOid();
    }

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
    private Map<Long, com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO> entityOidToDto = Maps.newHashMap();

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

    private float quoteFactor = MarketAnalysisUtils.QUOTE_FACTOR;
    private float liveMarketMoveCostFactor = MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR;

    // Add a cost of moving from source to destination.
    public static final float PLAN_MOVE_COST_FACTOR = 0.0f;
    public static final float CLOUD_QUOTE_FACTOR = 1;

    private final CommodityConverter commodityConverter;

    private final ActionInterpreter actionInterpreter;

    // A map of the topology entity dto id to its reserved instance coverage
    private final Map<Long, EntityReservedInstanceCoverage> projectedReservedInstanceCoverage = Maps.newHashMap();

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    /**
     * Utility to track errors encountered during conversion.
     */
    private final ConversionErrorCounts conversionErrorCounts = new ConversionErrorCounts();

    /**
     * Index that keeps scaling factors applied during conversion TO market entities, to allow
     * quick lookups to reverse scaling when converting FROM market entities.
     */
    private CommodityIndex commodityIndex;

    private final TierExcluder tierExcluder;

    private final ConsistentScalingHelper consistentScalingHelper;

    private final boolean enableSMA;

    /**
     * Constructor with includeGuaranteedBuyer parameter.
     *
     * @param topologyInfo Information about the topology.
     * @param includeGuaranteedBuyer whether to include guaranteed buyers (VDC, VPod, DPod) or not
     * @param quoteFactor to be used by move recommendations.
     * @param enableSMA the market generates compute scaling action for could vms if false.
     *                  the SMA (Stable Marriage Algorithm)  library generates them if true.
     * @param liveMarketMoveCostFactor used by the live market to control aggressiveness of move actions.
     * @param marketPriceTable market price table
     * @param commodityConverter the commodity converter
     * @param cloudCostData cloud cost data
     * @param commodityIndexFactory commodity index factory
     * @param tierExcluderFactory tierExcluderFactory
     * @param consistentScalingHelperFactory CSM helper factory
     * @param cloudTopology instance to look up topology relationships
     */
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             final boolean includeGuaranteedBuyer,
                             final float quoteFactor,
                             final boolean enableSMA,
                             final float liveMarketMoveCostFactor,
                             @Nonnull final MarketPriceTable marketPriceTable,
                             CommodityConverter commodityConverter,
                             final CloudCostData cloudCostData,
                             final CommodityIndexFactory commodityIndexFactory,
                             @Nonnull final TierExcluderFactory tierExcluderFactory,
                             @Nonnull final ConsistentScalingHelperFactory consistentScalingHelperFactory,
                             @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.cloudTopology = cloudTopology;
        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
        this.quoteFactor = quoteFactor;
        this.enableSMA = enableSMA;
        this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
        this.consistentScalingHelper = consistentScalingHelperFactory
            .newConsistentScalingHelper(topologyInfo, getShoppingListOidToInfos());
        this.commodityConverter = commodityConverter != null ?
                commodityConverter : new CommodityConverter(commodityTypeAllocator, commoditySpecMap,
                includeGuaranteedBuyer, dsBasedBicliquer, numConsumersOfSoldCommTable,
                conversionErrorCounts, consistentScalingHelper);
        this.tierExcluder = tierExcluderFactory.newExcluder(topologyInfo, this.commodityConverter,
            getShoppingListOidToInfos());
        this.cloudTc = new CloudTopologyConverter(unmodifiableEntityOidToDtoMap, topologyInfo,
                pmBasedBicliquer, dsBasedBicliquer, this.commodityConverter, azToRegionMap, businessAccounts,
                marketPriceTable, cloudCostData, tierExcluder, cloudTopology);
        this.commodityIndex = commodityIndexFactory.newIndex();
        this.actionInterpreter = new ActionInterpreter(this.commodityConverter, shoppingListOidToInfos,
                cloudTc,
                unmodifiableEntityOidToDtoMap,
                oidToProjectedTraderTOMap,
                cert,
                projectedReservedInstanceCoverage, tierExcluder);
    }


    /**
     * Constructor with includeGuaranteedBuyer parameter.
     *
     * @param topologyInfo Information about the topology.
     * @param includeGuaranteedBuyer whether to include guaranteed buyers (VDC, VPod, DPod) or not
     * @param quoteFactor to be used by move recommendations.
     * @param liveMarketMoveCostFactor used by the live market to control aggressiveness of move actions.
     * @param marketPriceTable the market price table
     * @param cloudCostData cloud cost data
     * @param commodityIndexFactory commodity index factory
     * @param tierExcluderFactory tier excluder factory
     * @param consistentScalingHelperFactory CSM helper factory
     */
    @VisibleForTesting
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             final boolean includeGuaranteedBuyer,
                             final float quoteFactor,
                             final float liveMarketMoveCostFactor,
                             @Nonnull final MarketPriceTable marketPriceTable,
                             @Nonnull final CloudCostData cloudCostData,
                             @Nonnull final CommodityIndexFactory commodityIndexFactory,
                             @NonNull final TierExcluderFactory tierExcluderFactory,
                             @Nonnull final ConsistentScalingHelperFactory
                                     consistentScalingHelperFactory) {
        this(topologyInfo, includeGuaranteedBuyer, quoteFactor, false, liveMarketMoveCostFactor,
            marketPriceTable, null, cloudCostData, commodityIndexFactory, tierExcluderFactory,
            consistentScalingHelperFactory, null);
    }

    @VisibleForTesting
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             final boolean includeGuaranteedBuyer,
                             final float quoteFactor,
                             final boolean enableSMA,
                             final float liveMarketMoveCostFactor,
                             @Nonnull final MarketPriceTable marketPriceTable,
                             @Nonnull CommodityConverter commodityConverter,
                             @Nonnull final CommodityIndexFactory commodityIndexFactory,
                             @NonNull final TierExcluderFactory tierExcluderFactory,
                             @Nonnull final ConsistentScalingHelperFactory
                                     consistentScalingHelperFactory) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.cloudTopology = null;
        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
        this.quoteFactor = quoteFactor;
        this.enableSMA = enableSMA;
        this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
        this.commodityConverter = commodityConverter;
        this.tierExcluder = tierExcluderFactory.newExcluder(topologyInfo, this.commodityConverter,
            getShoppingListOidToInfos());
        this.cloudTc = new CloudTopologyConverter(unmodifiableEntityOidToDtoMap, topologyInfo,
                pmBasedBicliquer, dsBasedBicliquer, commodityConverter, azToRegionMap,
                businessAccounts, marketPriceTable, null, tierExcluder, cloudTopology);
        this.commodityIndex = commodityIndexFactory.newIndex();
        this.actionInterpreter = new ActionInterpreter(commodityConverter, shoppingListOidToInfos,
                cloudTc,
            unmodifiableEntityOidToDtoMap,
            oidToProjectedTraderTOMap,
            cert,
            projectedReservedInstanceCoverage, tierExcluder);
        this.consistentScalingHelper = consistentScalingHelperFactory
            .newConsistentScalingHelper(topologyInfo, getShoppingListOidToInfos());
    }

    // Read only version
    protected final Map<Long, ShoppingListInfo> getShoppingListOidToInfos() {
        return Collections.unmodifiableMap(shoppingListOidToInfos);
    }


    private boolean isPlan() {
        return TopologyDTOUtil.isPlan(topologyInfo);
    }

    private boolean isOptimizeCloudPlan() {
        return TopologyDTOUtil.isOptimizeCloudPlan(topologyInfo);
    }

    /**
     * Convert a collection of common protobuf topology entity DTOs to analysis protobuf economy DTOs.
     * @param topology list of topology entity DTOs
     * @return set of economy DTOs
     */
    @Nonnull
    public Set<EconomyDTOs.TraderTO> convertToMarket(
                @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> topology) {
        // Initialize the template exclusion applicator
        tierExcluder.initialize(topology);
        // Initialize the consistent resizer
        consistentScalingHelper.initialize(topology);
        // TODO (roman, Jul 5 2018): We don't need to create a new entityOidToDto map.
        // We can have a helper class that will apply the skipped entity logic on the
        // original topology.
        conversionErrorCounts.startPhase(Phase.CONVERT_TO_MARKET);
        long convertToMarketStartTime = System.currentTimeMillis();
        try {
            for (TopologyDTO.TopologyEntityDTO entity : topology.values()) {
                int entityType = entity.getEntityType();
                if (MarketAnalysisUtils.SKIPPED_ENTITY_TYPES.contains(entityType)
                        || !includeByType(entityType)) {
                    logger.debug("Skipping trader creation for entity name = {}, entity type = {}, " +
                            "entity state = {}", entity.getDisplayName(),
                        EntityType.forNumber(entity.getEntityType()), entity.getEntityState());
                    skippedEntities.put(entity.getOid(), entity);
                } else {
                    // Allow creation of traderTOs for traders discarded due to state or entityType.
                    // We will remove them traderTO set after scoping but before sending to market.
                    if (SKIPPED_ENTITY_STATES.contains(entity.getEntityState())) {
                        skippedEntities.put(entity.getOid(), entity);
                    }
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
                        if (cloudTc.getAccountPricingIdFromBusinessAccount(entity.getOid()).isPresent()) {
                            cloudTc.insertIntoAccountPricingDataByBusinessAccountOidMap(entity.getOid(),
                                    cloudTc.getAccountPricingIdFromBusinessAccount(entity.getOid()).get());
                        }
                    }
                }
            }
            return convertToMarket();
        } catch (RuntimeException e) {
            //throw new RuntimeException("RuntimeException in convertToMarket(topology) ", e);
            logger.error("RuntimeException in convertToMarket(topology) ");
            throw e;
        } finally {
            conversionErrorCounts.endPhase();
            tierExcluder.clearStateNeededForConvertToMarket();
            long convertToMarketEndTime = System.currentTimeMillis();
            logger.info("Completed converting TopologyEntityDTOs to traderTOs. Time taken = {} seconds",
                ((double)(convertToMarketEndTime - convertToMarketStartTime)) / 1000);
        }
    }

    /**
     * Convert a map of common protobuf topology entity DTOs to analysis protobuf economy DTOs.
     * @return set of economy DTOs
     */
    @Nonnull
    private Set<EconomyDTOs.TraderTO> convertToMarket() {
        try {
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
            // Iterate over all scaling groups and compute top usage
            calculateScalingGroupUsageData(entityOidToDto);
            entityOidToDto.values().stream()
                    .filter(t -> TopologyConversionUtils.shouldConvertToTrader(t.getEntityType()))
                    .map(this::topologyDTOtoTraderTO)
                    .filter(Objects::nonNull)
                    .forEach(returnBuilder::add);

            logger.info("Converted topologyEntityDTOs to traderTOs");
            return returnBuilder.build();
        } catch (RuntimeException e) {
            logger.error("RuntimeException in convertToMarket");
            throw e;
        }
    }

    /**
     * Iterate over all commodities bought by a trader from a supplier and increment the
     * number of consumers associated with the commodities bought in the corresponding seller.
     *
     * <p>This information is stored in numConsumersOfSoldCommTable
     *
     */
    private void populateCommodityConsumesTable(TopologyDTO.TopologyEntityDTO dto) {
        // iterate over the commoditiesBought by a buyer on a per seller basis
        dto.getCommoditiesBoughtFromProvidersList().forEach(entry ->
            // for each commodityBought, we increment the number of consumers for the
            // corresponding commSold in numConsumersOfSoldCommTable
            entry.getCommodityBoughtList().stream()
                // Ignore inactive commodity bought.
                // TopologyEntityDTO with an inactive commodity bought is not considered as a consumer.
                .filter(CommodityBoughtDTO::getActive)
                .forEach(commDto -> {
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
     * @param wastedFileAnalysis wasted file analysis handler
     * @return list of {@link TopologyDTO.ProjectedTopologyEntity}s
     */
    @Nonnull
    public Map<Long, TopologyDTO.ProjectedTopologyEntity> convertFromMarket(
                @Nonnull final List<EconomyDTOs.TraderTO> projectedTraders,
                @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology,
                @Nonnull final PriceIndexMessage priceIndexMessage,
                @Nonnull final CloudCostData cloudCostData,
                @Nonnull final ReservedCapacityAnalysis reservedCapacityAnalysis,
                @Nonnull final WastedFilesAnalysis wastedFileAnalysis) {

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
                    traderTOtoTopologyDTO(projectedTrader, originalTopology, reservedCapacityAnalysis, projTraders, wastedFileAnalysis);
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
                final TraderTO originalTraderTO = oidToOriginalTraderTOMap.get(projectedTrader.getOid());
                // original trader can be null in case of provisioned entity
                if (originalTraderTO != null) {
                    final Optional<EntityReservedInstanceCoverage> originalRiCoverage =
                            cloudCostData.getRiCoverageForEntity(originalTraderTO.getOid());

                    // projected RI coverage does not support provisioned traders, due to requiring
                    // the original trader state for correct RI coverage capacity calculation. Currently,
                    // cloud entities cannot be provisioned.
                    calculateProjectedRiCoverage(originalTraderTO, projectedTrader, originalRiCoverage);
                }
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
     * @param originalTraderTO the original trader (before going into market). The original trader is
     *                         used to determine the projected trader's state (IDLE VMs will be converted
     *                         to ACTIVE in the market) and to determine whether the RI coverage has
     *                         changed.
     * @param projectedTraderTO the projected trader for which RI Coverage is to be created
     * @param originalRiCoverage the original trader's original RI Coverage (before going into market)
     */
    private void calculateProjectedRiCoverage(@Nonnull TraderTO originalTraderTO,
                                              @Nonnull TraderTO projectedTraderTO,
                                              @Nonnull Optional<EntityReservedInstanceCoverage> originalRiCoverage) {

        final Optional<Integer> projectedCoverageCapacity =
                cloudTc.getReservedInstanceCoverageCapacity(
                        projectedTraderTO, originalTraderTO.getState());
        if (projectedCoverageCapacity.map(c -> c > 0).orElse(false)) {
            final long entityId = projectedTraderTO.getOid();
            final EntityReservedInstanceCoverage.Builder riCoverageBuilder =
                    EntityReservedInstanceCoverage.newBuilder()
                            .setEntityCouponCapacity(projectedCoverageCapacity.get())
                            .setEntityId(entityId);
            if (isValidProjectedTrader(projectedTraderTO)) {
                // Are any of the shopping lists of the projected trader placed on discounted tier?
                final Optional<ShoppingListTO> projectedRiTierSl =
                    getShoppingListSuppliedByRiTier(projectedTraderTO, true);
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

                    final Optional<MarketTier> originalRiTierDestination =
                        originalTraderTO.getShoppingListsList()
                            .stream()
                            .map(sl -> sl.getSupplier()).map(traderId -> cloudTc.getMarketTier(traderId))
                            .filter(Objects::nonNull).filter(mt -> mt.hasRIDiscount())
                            .findFirst();

                    if (!originalRiTierDestination.isPresent() ||
                        !originalRiTierDestination.get().equals(projectedRiTierDestination)) {
                        // Entity is initially placed on an riTier
                        // OR has moved from a regular tier to an riTier
                        // OR from one riTer to another.
                        // So, in all these cases, create a new RICoverage object.
                        // The information of the number of coupons to use is present in the coupon
                        // commodity bought of the trader
                        final Map<Long, Double> riIdtoCouponsOfRIUsed = projectedRiTierDestination
                            .useCoupons(entityId, projectedCouponCommBought.get().getQuantity());
                        riCoverageBuilder.putAllCouponsCoveredByRi(riIdtoCouponsOfRIUsed);
                    } else {
                        // Entity stayed on the same RI Tier. Check if the coverage changed.
                        // If we are in this block, originalTraderTO cannot be null.
                        if (!originalRiCoverage.isPresent()) {
                            logger.error("{} does not have original RI coverage", originalTraderTO.getDebugInfoNeverUseInCode());
                            return;
                        }

                        float projectedNumberOfCouponsBought = projectedCouponCommBought.get().getQuantity();
                        float originalNumberOfCouponsBought = getTotalNumberOfCouponsCovered(originalRiCoverage.get());
                        // If original == projected, the original claimed RIs were not relinquished
                        // in relinquishCoupons(). Therefore, the projectedRiTierDestination does
                        // not need to be updated here
                        if (!TopologyConversionUtils.areFloatsEqual(
                            projectedCouponCommBought.get().getQuantity(),
                            originalNumberOfCouponsBought)) {
                            // Coverage changed. Create a new RICoverage object
                            final Map<Long, Double> riIdtoCouponsOfRIUsed = projectedRiTierDestination
                                .useCoupons(entityId, projectedNumberOfCouponsBought);
                            riCoverageBuilder.putAllCouponsCoveredByRi(riIdtoCouponsOfRIUsed);
                        } else {
                            // Coverage did not change. Use the original ri coverage.
                            riCoverageBuilder.putAllCouponsCoveredByRi(
                                originalRiCoverage.get().getCouponsCoveredByRiMap());
                        }
                    }
                }
            }

            projectedReservedInstanceCoverage.put(entityId, riCoverageBuilder.build());
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
            if (originalTrader != null && isValidProjectedTrader(projectedTrader)) {
                // If the VM was using an RI before going into market, then original trader will
                // have a shopping list supplied by RIDiscountedMarketTier because in this case
                // while constructing shopping lists of original trader, we make the supplier of
                // the compute shopping list as the trader representing RIDiscountedMarketTier
                Optional<ShoppingListTO> originalRiTierSl = getShoppingListSuppliedByRiTier(originalTrader, false);
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
                    Optional<ShoppingListTO> projectedRiTierSl = getShoppingListSuppliedByRiTier(projectedTrader, true);
                    if (projectedRiTierSl.isPresent()) {
                        if (projectedRiTierSl.get().getCouponId() != originalRiTierSl.get().getSupplier()) {
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
                            if (!TopologyConversionUtils.areFloatsEqual(
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
     * @param isProjectedTrader is the trader a projected trader
     * @return Optional of the ShoppingListTO
     */
    private Optional<ShoppingListTO> getShoppingListSuppliedByRiTier(@Nonnull TraderTO trader, boolean isProjectedTrader) {
        if (isProjectedTrader) {
            return trader.getShoppingListsList().stream().filter(sl -> sl.hasCouponId()).findFirst();
        } else {
            return trader.getShoppingListsList().stream().filter(sl -> sl.hasSupplier() &&
                cloudTc.getRiDiscountedMarketTier(sl.getSupplier()).isPresent()).findFirst();
        }
    }

    /**
     * Is the projectedTraderTO valid?
     * It is considered invalid if its compute SL is supplied by RI. It should only be supplied
     * by a regular compute tier. Inside m2, we replace the CBTP by TP in AnalysisToProtobuf::replaceNewSupplier.
     *
     * @param projectedTraderTO the trader to check if it is valid
     * @return True if the projected trader is valid. False otherwise.
     */
    private boolean isValidProjectedTrader(TraderTO projectedTraderTO) {
        Optional<MarketTier> computeSupplier = cloudTc.getComputeTier(projectedTraderTO);
        if (computeSupplier.isPresent() && computeSupplier.get().hasRIDiscount()) {
            logger.error("Projected trader for {} has compute SL supplied by " +
                    "RI {}. This should not happen.",
                projectedTraderTO.getDebugInfoNeverUseInCode(), computeSupplier.get().getDisplayName());
            return false;
        }
        return true;
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
     * Interpret stream of market-specific {@link ActionTO} as a stream of topology-specific
     * {@Action} proto for use by the rest of the system.
     *
     * <p>It is vital to use the same {@link TopologyConverter} that converted
     * the topology into market-specific objects
     * to interpret the resulting actions in a way that makes sense.</p>
     *
     * @param actionTOs A stream of {@link ActionTO} describing the action recommendations
     *                 generated by the market.
     * @param projectedTopology The projected topology. All entities involved in the action are
     *                          expected to be in the projected topology.
     * @param originalCloudTopology {@link CloudTopology} of the original {@link TopologyEntityDTO}s  received by Analysis
     * @param projectedCosts  A map of id of projected topologyEntityDTO -> {@link CostJournal} for the entity with that ID.
     * @param topologyCostCalculator the topologyCostCalculator is used to calculate costs of entities
     * @return The {@link Action} describing the recommendation in a topology-specific way.
     */
    @Nonnull
    public List<Action> interpretAllActions(@NonNull final List<ActionTO> actionTOs,
                                              @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                              @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                              @Nonnull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
                                              @Nonnull TopologyCostCalculator topologyCostCalculator) {
        // Before beginning to interpret actions, compute the reason settings for the tier exclusion
        // actions
        tierExcluder.computeReasonSettings(actionTOs, originalCloudTopology);
        List<Action> actions = Lists.newArrayList();
        actionTOs.forEach(actionTO -> {
            Optional<Action> action = interpretAction(actionTO, projectedTopology,
                originalCloudTopology, projectedCosts, topologyCostCalculator);
            action.ifPresent(actions::add);
        });
        tierExcluder.clearStateNeededForActionInterpretation();
        return actions;
    }

    /**
     * Interpret the market-specific {@link ActionTO} as a topology-specific {@Action} proto
     * for use by the rest of the system.
     *
     * <p>It is vital to use the same {@link TopologyConverter} that converted
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
    @NonNull
    Optional<Action> interpretAction(@NonNull final ActionTO actionTO,
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
            CommoditiesBoughtFromProvider.newBuilder().addAllCommodityBought(commList);
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
     * @param projTraders projected traders
     * @param wastedFileAnalysis handler for wasted file analysis
     * @return set of {@link TopologyDTO.TopologyEntityDTO}s
     */
    private Set<TopologyDTO.TopologyEntityDTO> traderTOtoTopologyDTO(EconomyDTOs.TraderTO traderTO,
                    @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> traderOidToEntityDTO,
                    @Nonnull final ReservedCapacityAnalysis reservedCapacityAnalysis,
                    @Nonnull final Map<Long, EconomyDTOs.TraderTO> projTraders,
                    @Nonnull final WastedFilesAnalysis wastedFileAnalysis) {
        Set<TopologyDTO.TopologyEntityDTO> topologyEntityDTOs = Sets.newHashSet();
        if (cloudTc.isMarketTier(traderTO.getOid())) {
            // Tiers and regions are added from the original topology into the projected traders
            // because there can be no change to these entities by market.
            return topologyEntityDTOs;
        }
        List<CommoditiesBoughtFromProvider> topoDTOCommonBoughtGrouping = new ArrayList<>();
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
                } else if (tierExcluder.isCommSpecTypeForTierExclusion(commBought.getSpecification().getType())) {
                    // If the commodity bought was created by market-component for tier exclusion,
                    // then do not include it in the projected topology.
                    continue;
                } else {
                    commBoughtTOtoCommBoughtDTO(traderTO.getOid(), sl.getSupplier(), sl.getOid(),
                        commBought, reservedCapacityAnalysis, originalEntity).ifPresent(commList::add);
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

        // Get dspm, datastore and inactive commodity sold from the original trader,
        // add them to the projected topology entity DTO.
        entityDTOBuilder.addAllCommoditySoldList(
            originalEntity.getCommoditySoldListList().stream()
                .filter(commSold ->
                    AnalysisUtil.DSPM_OR_DATASTORE.contains(commSold.getCommodityType().getType()) ||
                    !commSold.getActive())
                .collect(Collectors.toSet()));

        // handle 'delete wasted file analysis' to update entity from market
        wastedFileAnalysis.getStorageAmountReleasedForOid(traderTO.getOid())
            .ifPresent(stAmt -> {
                final long stAmtToReleaseInMB = stAmt / Units.NUM_OF_KB_IN_MB;
                entityDTOBuilder.getCommoditySoldListBuilderList().stream()
                    .filter(commSold -> commSold.getCommodityType().getType() == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                    .findFirst()
                    .ifPresent(storageCommSold ->
                        storageCommSold.setUsed(Math.max(0, storageCommSold.getUsed() - stAmtToReleaseInMB)));
            });

        TopologyEntityDTO entityDTO = entityDTOBuilder.build();
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
        for (CommoditiesBoughtFromProvider commBoughtGrouping :
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
                    Long regionCommSpec = cloudTc.getRegionCommTypeIntFromShoppingList(traderTO);
                    TopologyEntityDTO destinationRegion;
                    if (destinationPrimaryMarketTier instanceof SingleRegionMarketTier) {
                        destinationRegion = ((SingleRegionMarketTier)destinationPrimaryMarketTier).getRegion();
                    } else {
                        if (regionCommSpec != null) {
                            destinationRegion = unmodifiableEntityOidToDtoMap.get(regionCommSpec);
                        } else {
                            destinationRegion = sourceRegion;
                        }
                    }
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
                    if (destAZOrRegion != null) {
                        ConnectedEntity az = ConnectedEntity.newBuilder()
                                .setConnectedEntityId(destAZOrRegion.getOid())
                                .setConnectedEntityType(destAZOrRegion.getEntityType())
                                .setConnectionType(ConnectionType.NORMAL_CONNECTION).build();
                        connectedEntities.add(az);
                    }
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
     * utilization. Th same resize demand will be considered to calculate new quantities required
     * for both Resize-up and Resize-down. This will be consistent for both use cases where there
     * is a driving sold commodity and no driving sold commodity.
     * @param topologyDTO topology entity DTO as resize target
     * @param commBought commodity bought to be resized
     * @param providerOid the oid of the seller of the shopping list
     * @return an array of two elements, the first element is new used value,
     * the second is the new peak value
     */
    protected double[] getResizedCapacity(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO,
            @Nonnull final TopologyDTO.CommodityBoughtDTO commBought,
            @Nullable final Long providerOid) {

        logger.debug("Recalculating new capacity for {}", topologyDTO.getDisplayName());
        float histUsed =
                CommodityConverter.getHistoricalUsedOrPeak(commBought, TopologyDTO.CommodityBoughtDTO::getUsed,
                        TopologyDTO.CommodityBoughtDTO::getHistoricalUsed);
        float histPeak =
                CommodityConverter.getHistoricalUsedOrPeak(commBought, TopologyDTO.CommodityBoughtDTO::getPeak,
                        TopologyDTO.CommodityBoughtDTO::getHistoricalPeak);

        // TODO: Need to add check for Cloud migration here. This will apply to Cloud Migration too.
        if (topologyDTO.getEnvironmentType() != EnvironmentType.CLOUD) {
            return getOnPremResizedCapacity(histUsed, histPeak, commBought, providerOid, topologyDTO);
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
                float histUsage = (float)commoditySoldDTO.getUsed();
                if (commoditySoldDTO.hasHistoricalUsed()) {
                    if (commoditySoldDTO.getHistoricalUsed().hasPercentile() &&
                            commoditySoldDTO.getIsResizeable()) {
                        final float percentile = (float)commoditySoldDTO.getHistoricalUsed().getPercentile();
                        logger.debug("Using percentile value {} to recalculate capacity for {} in {}",
                                percentile, commoditySoldDTO.getCommodityType().getType(),
                                topologyDTO.getDisplayName());
                        histUsage =  percentile * (float)commoditySoldDTO.getCapacity();

                    } else if (commoditySoldDTO.getHistoricalUsed().hasHistUtilization()) {
                        // if not then hist utilization which is the historical used value.
                        histUsage =
                                (float)commoditySoldDTO.getHistoricalUsed().getHistUtilization();
                        logger.debug("Using historical value {} to recalculate capacity for {} in {}",
                                histUsage, commoditySoldDTO.getCommodityType().getType(),
                                topologyDTO.getDisplayName());
                    }
                }
                final double[] resizedQuantity =
                        calculateResizedQuantity(histUsage, histUsage,
                                commoditySoldDTO.getUsed(), commoditySoldDTO.getPeak(),
                                commoditySoldDTO.getCapacity(),
                                commoditySoldDTO.getResizeTargetUtilization(), true);
                cert.logCommodityResize(topologyDTO.getOid(), commoditySoldDTO.getCommodityType(),
                        resizedQuantity[0] - commoditySoldDTO.getCapacity());
                logger.debug(
                        "Using a peak used of {} for driving commodity type {} for entity {}.",
                        resizedQuantity[1], commoditySoldDTO.getCommodityType().getType(),
                        topologyDTO.getDisplayName());
                return resizedQuantity;
            }
        } else if (providerOid != null &&
                TopologyConversionConstants.BOUGHT_COMMODITIES_RESIZED.contains(
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
                if (commoditySoldDTO.isPresent() && commoditySoldDTO.get().getIsResizeable()) {
                    // We want to use the historical used (already smoothened) for both the resize-up
                    // and resize-down demand calculations. We do not want to consider the
                    // historical max or the peaks to avoid a one-time historic max value to cause
                    // resize decisions.
                    final double[] resizedQuantity = calculateResizedQuantity(histUsed, histUsed,
                            commBought.getUsed(), histPeak,
                            commoditySoldDTO.get().getCapacity(),
                            commBought.getResizeTargetUtilization(), false);
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

    /**
     * Calculates the new resized  capacities for the bought commodity based on historical
     * demands.
     * @param used Bought commodity historical used value.
     * @param peak Bought commodity peak value.
     * @param commBought Bought commodity.
     * @param providerOid Oid of the provider DTO for this commodity.
     * @param topologyDTO the TopologyDTO buying this commodity.
     * @return the new calculated capacities for the bought commodity.
     */
    private double[] getOnPremResizedCapacity(final float used,
                                              final float peak,
                                              @Nonnull final TopologyDTO.CommodityBoughtDTO commBought,
                                              final Long providerOid,
                                              @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO) {
        // An example use case for the condition below is the
        // VDI use case where we need to apply the target Util on the percentile.
        float histUsed = used;
        float histPeak = peak;
        if (commBought.hasHistoricalUsed()
                && commBought.getHistoricalUsed().hasPercentile()) {
            final TopologyEntityDTO providerTopologyEntity = entityOidToDto.get(providerOid);
            if (providerTopologyEntity == null) {
                logger.warn("Could not find provider for entity {} with id {}." +
                                " Using percentile used and peak without " +
                                "applying the target utilization", topologyDTO.getDisplayName(),
                        providerOid);

                return new double[]{histUsed, histPeak};
            }
            // Find the corresponding sold commodity. For example, in the VDI use case,
            // this would be the image commodity sold by the desktop pool.
            final List<TopologyDTO.CommoditySoldDTO> commoditiesSoldByProvider =
                    providerTopologyEntity.getCommoditySoldListList()
                            .stream()
                            .filter(c -> c.getCommodityType().getType()
                                    == commBought.getCommodityType().getType())
                            .collect(Collectors.toList());
            if (commoditiesSoldByProvider.size() != 1 || commoditiesSoldByProvider.get(0) == null) {
                logger.warn("Could not find corresponding sold commodity from provider {} for" +
                                "type {},  for entity {}. Using percentile used and peak without" +
                                "applying the target utilization", providerTopologyEntity.getDisplayName(),
                        commBought.getCommodityType().getType(), topologyDTO.getDisplayName());
            } else {
                float targetUtil = (float)commBought.getResizeTargetUtilization();
                double capacity = commoditiesSoldByProvider.get(0).getCapacity();
                histUsed = (float)(histUsed / targetUtil * capacity);
                histPeak = (float)(histPeak / targetUtil * capacity);
            }
        }
        return new double[]{histUsed, histPeak};
    }

    private double[] calculateResizedQuantity(double resizeUpDemand, double resizeDownDemand,
            double used, double peak, double capacity, double targetUtil, boolean isSoldCommodity) {
        if (targetUtil <= 0.0) {
            targetUtil = EntitySettingSpecs.UtilTarget.getSettingSpec()
                    .getNumericSettingValueType()
                    .getDefault() / 100D;
        }

        // For cloud entities / cloud migration plans, we do not use the peakQuantity.
        // We only use the quantity values inside M2
        final double peakQuantity = Math.max(used, peak) / targetUtil;
        double quantity = used;
        if (Math.ceil(resizeUpDemand / targetUtil) > capacity) {
            quantity = resizeUpDemand / targetUtil;
        } else if (resizeDownDemand > 0 && Math.ceil(resizeDownDemand / targetUtil) < capacity) {
            quantity = resizeDownDemand / targetUtil;
        } else if (isSoldCommodity) {
            /* Sold commodities like vMem may sometimes have a zero usage
            when the probe does not send in metrics. But, this is not applicable for
            the bought commodities like the throughput commodities. */
            quantity = capacity;
        }
        return new double[]{quantity, peakQuantity};
    }

    /**
     * Calculates the weighted usage which will be used for resize down for cloud resource.
     *
     * @param commodity the {@link TopologyDTO.CommoditySoldDTO}
     * @return weighted used value calculated from avg, peak and max
     */
    @SuppressWarnings("unused")
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
     * @param originalEntity the original entity DTO
     * @return {@link TopologyDTO.CommoditySoldDTO} that the trader sells
     */
    @Nonnull
    private Optional<TopologyDTO.CommodityBoughtDTO> commBoughtTOtoCommBoughtDTO(
            final long traderOid, final long supplierOid, final long slOid,
            @Nonnull final CommodityBoughtTO commBoughtTO,
            @Nonnull final ReservedCapacityAnalysis reservedCapacityAnalysis,
            final TopologyEntityDTO originalEntity) {


        float peak = commBoughtTO.getPeakQuantity();
        if (peak < 0) {
            conversionErrorCounts.recordError(ErrorCategory.PEAK_NEGATIVE,
                commodityConverter.getCommodityName(commBoughtTO.getSpecification().getType()));
            logger.trace("The entity with negative peak is {} (buying {} from {})",
                traderOid, commBoughtTO, supplierOid);
            peak = 0;
        }

        final float peakQuantity = peak; // It must be final

        long volumeId = shoppingListOidToInfos.get(slOid) != null
                && shoppingListOidToInfos.get(slOid).getResourceId().isPresent() ?
                shoppingListOidToInfos.get(slOid).getResourceId().get() : 0;
        return commodityConverter.economyToTopologyCommodity(commBoughtTO.getSpecification())
                .map(commType -> {
                    Optional<CommodityBoughtDTO> originalCommodityBoughtDTO =
                            commodityIndex.getCommBought(traderOid, supplierOid, commType, volumeId);
                    float currentUsage = getOriginalUsedValue(commBoughtTO, traderOid,
                            supplierOid, commType, volumeId, originalEntity);
                    final Builder builder = CommodityBoughtDTO.newBuilder();
                    builder.setUsed(reverseScaleComm(currentUsage, originalCommodityBoughtDTO,
                                    CommodityBoughtDTO::getScalingFactor))
                    .setReservedCapacity(reservedCapacityAnalysis.getReservedCapacity(traderOid, commType))
                    .setCommodityType(commType)
                    .setPeak(peakQuantity);
                    if (originalCommodityBoughtDTO.isPresent()
                            && originalCommodityBoughtDTO.get().hasHistoricalUsed()
                            && originalCommodityBoughtDTO.get().getHistoricalUsed().hasPercentile()) {
                        //TODO Lakshmi - Set the projected percentile as exsitingPercentile * (existing capacity/new caapcity)
                        // this wil be required by the VDI Image commodites.
                        builder.setHistoricalUsed(HistoricalValues.newBuilder()
                                .setPercentile(originalCommodityBoughtDTO.get().getHistoricalUsed().getPercentile())
                                .build());
                    }
                    return builder.build(); });
    }

    /**
     * Find the original used value for a given Bought commodity TO.
     * @param commBoughtTO - BoughtTO for which we want the original used value.
     * @param traderOid Trader Oid
     * @param supplierOid Provider Oid
     * @param commType commodity type
     * @param volumeId Volume Id
     * @param originalEntity the original topology entity DTO for the trader.
     * @return the used value
     */
    private float getOriginalUsedValue(final CommodityBoughtTO commBoughtTO, final long traderOid,
                                       final long supplierOid, final CommodityType commType,
                                       final long volumeId,
                                       final TopologyEntityDTO originalEntity) {
        float currentUsage = commBoughtTO.getQuantity();
        // If this is a cloud entity, find the original bought commodity from supplier.
        if (cloudTc.isMarketTier(supplierOid)) {
            if (cloudTc.getMarketTier(supplierOid).getTier() != null &&
                    TopologyConversionConstants.BOUGHT_COMMODITIES_RESIZED
                            .contains(commType.getType())) {
                //  Retrieve old bought commodity,given the old provider id
                //  and original entity
                final int providerType =
                        cloudTc.getMarketTier(supplierOid).getTier().getEntityType();
                final Set<TopologyEntityDTO> providerDTO =
                        cloudTc.getTopologyEntityDTOProvidersOfType(originalEntity,
                                providerType);
                /* The cloudTc.getTopologyEntityDTOProvidersOfType returns a list in
                cases where there maybe multiple providers for the same provider type
                as in the case of a VM buying from multiple storages. We don't need to
                find the original bought commodity to restore the quantity in this case.
                */
                if (providerDTO.size() == 1) {
                    Optional<CommodityBoughtDTO> originalCommodityBoughtDTO =
                            commodityIndex.getCommBought(traderOid,
                                    providerDTO.stream().findFirst().get().getOid(),
                                    commType, volumeId);
                    if (originalCommodityBoughtDTO.isPresent()) {
                        currentUsage = (float)originalCommodityBoughtDTO.get().getUsed();
                    }
                }
            } else if (cloudTc.getMarketTier(supplierOid).getTier() == null) {
                logger.warn("Unable to find providerType to retrieve original" +
                                " commodityBought for commodity {} in trader {}." +
                                " Using usage as received from trader from market.",
                        commBoughtTO.getSpecification().toString(), traderOid);
            }
        }
        return currentUsage;
    }

    /**
     * Convert a topology entity DTO to a trader TO.
     * @param topologyDTO topology entity DTO to process
     * @return the converted trader
     */
    @VisibleForTesting
     EconomyDTOs.TraderTO topologyDTOtoTraderTO(@Nonnull final TopologyEntityDTO topologyDTO) {
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
            if ((isPlan() && entityType == EntityType.VIRTUAL_MACHINE_VALUE)) {
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
            TraderSettingsTO.Builder settingsBuilder = TopologyConversionUtils
                    .createCommonTraderSettingsTOBuilder(topologyDTO, unmodifiableEntityOidToDtoMap);
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
                    .setQuoteFactor(isEntityFromCloud ? CLOUD_QUOTE_FACTOR : quoteFactor)
                    .setMoveCostFactor((isPlan() || isEntityFromCloud)
                            ? PLAN_MOVE_COST_FACTOR
                            : liveMarketMoveCostFactor)
                    .setProviderMustClone(isProviderMustClone);

            //compute biclique IDs for this entity, the clique list will be used only for
            // shop together placement, so pmBasedBicliquer is called
            Set<Long> allCliques = pmBasedBicliquer.getBcIDs(String.valueOf(topologyDTO.getOid()));

            Pair<List<EconomyDTOs.ShoppingListTO>, Long> p =
                    createAllShoppingListsAndReturnProviderId(topologyDTO);
            List<EconomyDTOs.ShoppingListTO> shoppingLists = p.first;
            TraderTO.Builder traderDTOBuilder = TraderTO.newBuilder()
                    // Type and Oid are the same in the topology DTOs and economy DTOs
                    .setOid(topologyDTO.getOid())
                    .setType(entityType)
                    .setState(state)
                    .setDebugInfoNeverUseInCode(entityDebugInfo(topologyDTO))
                    .addAllCommoditiesSold(createAllCommoditySoldTO(topologyDTO))
                    .addAllShoppingLists(shoppingLists)
                    .addAllCliques(allCliques);
            createCurrentContext(topologyDTO, p.second)
                .ifPresent(settingsBuilder::setCurrentContext);
            consistentScalingHelper.getScalingGroupId(topologyDTO.getOid())
                .ifPresent(traderDTOBuilder::setScalingGroupId);
            traderDTOBuilder.setSettings(settingsBuilder);
            traderDTO = traderDTOBuilder.build();
            oidToOriginalTraderTOMap.put(traderDTO.getOid(), traderDTO);
        } catch (RuntimeException e) {
            logger.error(entityDebugInfo(topologyDTO) + " could not be converted to traderTO:", e);
            throw e;
        }
        return traderDTO;
    }

    private Optional<Context.Builder> createCurrentContext(final TopologyEntityDTO topologyDTO,
                                                           final Long providerId) {
        final TopologyEntityDTO businessAccount = cloudEntityToBusinessAccount.get(topologyDTO);
        if (businessAccount == null) {
            return Optional.empty();
        }
        final Context.Builder result = Context.newBuilder()
                .setBalanceAccount(createBalanceAccountFromBusinessAccount(businessAccount));
        final TopologyEntityDTO region = cloudTc.getRegionOfCloudConsumer(topologyDTO);
        setIfPresent(region, result::setRegionId);
        final TopologyEntityDTO zone = cloudTc.getAZOfCloudConsumer(topologyDTO);
        setIfPresent(zone, result::setZoneId);
        Optional<EntityReservedInstanceCoverage> riCoverage =
                cloudTc.getRiCoverageForEntity(topologyDTO.getOid());
        if (riCoverage.isPresent() && providerId != null) {
            CoverageEntry entry = CoverageEntry.newBuilder()
                    .setProviderId(providerId)
                    .setTotalRequestedCoupons(getTotalNumberOfCouponsRequested(topologyDTO))
                    .setTotalAllocatedCoupons(getTotalNumberOfCouponsCovered(riCoverage.get()))
                    .build();
            result.addFamilyBasedCoverage(entry);
        }
        return Optional.of(result);
    }

    private float getTotalNumberOfCouponsRequested(TopologyEntityDTO entity) {
        return entity.getCommoditiesBoughtFromProvidersList().stream().filter(CommoditiesBoughtFromProvider::hasProviderId)
                .filter(entry -> entry.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE)
                .map(entry -> entityOidToDto.get(entry.getProviderId()))
                .map(TopologyEntityDTO::getTypeSpecificInfo)
                .map(TopologyDTO.TypeSpecificInfo::getComputeTier)
                .mapToLong(TopologyDTO.TypeSpecificInfo.ComputeTierInfo::getNumCoupons)
                .sum();
    }

    private void setIfPresent(final TopologyEntityDTO topologyDTO,
                                  Function<Long, Context.Builder> setter) {
        if (topologyDTO != null) {
            setter.apply(topologyDTO.getOid());
        }
    }

    @Nonnull
    private BalanceAccountDTO createBalanceAccountFromBusinessAccount(
            TopologyEntityDTO businessAccount) {
        // We create default balance account
        final double defaultBudgetValue = 100000000d;
        final float spent = 0f;
        final Optional<AccountPricingData<TopologyEntityDTO>> accountPricingData =
                cloudTc.getAccountPricingIdFromBusinessAccount(businessAccount.getOid());
        // Set the account pricing data oid on the balance account. If it is not found,
        // have the VM shop for its own business account id.
        final long priceId = accountPricingData.map(AccountPricingData::getAccountPricingDataOid)
                .orElse(businessAccount.getOid());
        return BalanceAccountDTO.newBuilder().setBudget(defaultBudgetValue).setSpent(spent)
                .setPriceId(priceId)
                .setId(businessAccount.getOid()).build();
    }

    private @Nonnull List<CommoditySoldTO> createAllCommoditySoldTO(@Nonnull TopologyEntityDTO topologyDTO) {
        List<CommoditySoldTO> commSoldTOList = new ArrayList<>();
        commSoldTOList.addAll(commodityConverter.commoditiesSoldList(topologyDTO));
        commSoldTOList.addAll(commodityConverter.bcCommoditiesSold(topologyDTO.getOid()));
        return commSoldTOList;
    }

    /**
     * Construct a string that can be used for debug purposes.
     *
     * <p>The debug info format should be: EntityType|OID|DisplayName
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
     * @return list of shopping lists bought by the corresponding trader, and the provider ID of the
     * compute tier, or null if not present in the shopping lists.
     */
    @Nonnull
    private Pair<List<ShoppingListTO>, Long> createAllShoppingListsAndReturnProviderId(
        @Nonnull final TopologyEntityDTO topologyEntity) {
        Optional<ScalingGroupUsage> scalingGroupUsage =
            consistentScalingHelper.getScalingGroupUsage(topologyEntity.getOid());
        // used for the case when a plan VM is unplaced
        Map<Long, Long> providers = oldProviders(topologyEntity);
        List<ShoppingListTO> shoppingLists = new ArrayList<>();
        Long computeTierProviderId = null;
        // Sort the commBoughtGroupings based on provider type and then by volume id so
        // that the input into analysis is consistent every cycle
        List<CommoditiesBoughtFromProvider> sortedCommBoughtGrouping = topologyEntity.getCommoditiesBoughtFromProvidersList().stream()
                .sorted(Comparator.comparing(CommoditiesBoughtFromProvider::getProviderEntityType)
                .thenComparing(CommoditiesBoughtFromProvider::getVolumeId)).collect(Collectors.toList());
        for (TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider commBoughtGrouping : sortedCommBoughtGrouping) {
            // skip converting shoppinglist that buys from VDC
            // TODO: we also skip the sl that consumes AZ which contains Zone commodity because zonal RI is not yet supported
            if (includeByType(commBoughtGrouping.getProviderEntityType())
                    && commBoughtGrouping.getProviderEntityType() != EntityType.AVAILABILITY_ZONE_VALUE) {
                Pair<Long, Class> providerId;
                if (scalingGroupUsage.isPresent() && commBoughtGrouping.getProviderEntityType() ==
                        EntityType.COMPUTE_TIER_VALUE) {
                    providerId = scalingGroupUsage.get().getProviderId(topologyEntity.getOid());
                } else {
                    providerId = getProviderId(commBoughtGrouping, topologyEntity);
                }
                if (providerId.second == RiDiscountedMarketTier.class) {
                    computeTierProviderId = providerId.first;
                }
                shoppingLists.add(
                    createShoppingList(
                        topologyEntity,
                        topologyEntity.getEntityType(),
                        topologyEntity.getAnalysisSettings().getShopTogether(),
                        providerId.first,
                        commBoughtGrouping,
                        providers,
                        scalingGroupUsage));
            }
        }
        return new Pair<>(shoppingLists, computeTierProviderId);
    }

    /**
     * Calculate usage data for all entities in each scaling group and return a map from entity
     * OID to a map from commodity type to top usage for the commodity.  This usage data is stored
     * in the consistent scaling helper instance.
     * @param topologyEntityDTOMap map of OIDs to their topology entity DTOs.
     */
    @Nonnull
    private void
    calculateScalingGroupUsageData(final Map<Long, TopologyEntityDTO> topologyEntityDTOMap) {
        if (logger.isTraceEnabled()) {
            logger.trace("calculateScalingGroupUsageData start");
        }
        for (ScalingGroup sg : consistentScalingHelper.getGroups()) {
            if (!sg.isCloudGroup()) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Scaling group {} not cloud - skipping usage leveling",
                        sg.getName());
                }
                continue;
            }
            ScalingGroupUsage usage = sg.getScalingGroupUsage();
            for (Long oid : sg.getMembers()) {
                TopologyEntityDTO topologyEntity = topologyEntityDTOMap.get(oid);
                if (topologyEntity == null ||
                    !TopologyConversionUtils.shouldConvertToTrader(topologyEntity.getEntityType())) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Entity {} in scaling group {} not convertible - skipping usage leveling",
                            topologyEntity == null ? "UNKNOWN" : topologyEntity.getDisplayName(),
                            sg.getName());
                    }
                    continue;
                }
                for (CommoditiesBoughtFromProvider commBoughtGrouping :
                    topologyEntity.getCommoditiesBoughtFromProvidersList()) {
                    if (commBoughtGrouping.getProviderEntityType() != EntityType.COMPUTE_TIER_VALUE) {
                        continue;
                    }
                    Pair<Long, Class> providerId = getProviderId(commBoughtGrouping, topologyEntity);
                    usage.setProviderId(oid, providerId);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Setting {} scaling group usage data provider id to {}, {}",
                            topologyEntity.getDisplayName(), providerId.first, providerId.second);
                    }
                    for (CommodityBoughtDTO commBought : commBoughtGrouping.getCommodityBoughtList()) {
                        if (!commBought.getActive() || !consistentScalingHelper
                                .isCommodityConsistentlyScalable(commBought.getCommodityType())) {
                            continue;
                        }
                        usage.addUsage(commBought, getCommBoughtQuantities(topologyEntity,
                                commBought, providerId.first));
                    }
                    // We've processed the grouping with the compute tier provider, so we're done.
                    break;
                }
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("calculateScalingGroupUsageData end");
        }
    }

    /**
     * Entity type is included in the converted topology if {@link #includeGuaranteedBuyer} is
     * true (in which case it is included regardless of its actual type), or else if the type
     * is not in the list of {@link AnalysisUtil#{GUARANTEED_BUYER_TYPES}. This method is used
     * to decide whether to include an entity in the topology by its type, and whether to
     * include a shopping list by its provider type.
     *
     * @param entityType the type of entity to consider for inclusion in the converted topology
     * @return whether to include this entity type in the converted topology
     */
    private boolean includeByType(int entityType) {
        return includeGuaranteedBuyer
            || !MarketAnalysisUtils.GUARANTEED_BUYER_TYPES.contains(entityType);
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
     * @param scalingGroupUsage cached usage data for buyers in scaling groups
     * @return a shopping list between the buyer and seller
     */
    @Nonnull
    private EconomyDTOs.ShoppingListTO createShoppingList(
            final TopologyEntityDTO buyer,
            final int entityType,
            final boolean shopTogether,
            @Nullable final Long providerOid,
            @Nonnull final CommoditiesBoughtFromProvider commBoughtGrouping,
            final Map<Long, Long> providers,
            final Optional<ScalingGroupUsage> scalingGroupUsage) {
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
                    shopTogether, providers, scalingGroupUsage))
            .filter(Objects::nonNull) // Null for DSPMAccess/Datastore and shop-together
            .collect(Collectors.toSet());
        boolean addGroupFactor = false;
        if (cloudTc.isMarketTier(providerOid)) {
            // For cloud buyers, add biClique comm bought because we skip them in
            // convertCommodityBought method
            if (!shopTogether) {
                createBcCommodityBoughtForCloudEntity(providerOid, buyerOid).forEach(values::add);
            }
            // Create DC comm bought
            createDCCommodityBoughtForCloudEntity(providerOid, buyerOid).ifPresent(values::add);
            // Create Coupon Comm
            Optional<CommodityBoughtTO> coupon = createCouponCommodityBoughtForCloudEntity(providerOid, buyerOid);
            if (coupon.isPresent()) {
                values.add(coupon.get());
                addGroupFactor = true;
            }
            // Create template exclusion commodity bought
            values.addAll(createTierExclusionCommodityBoughtForCloudEntity(providerOid, buyerOid));
        }
        final long id = shoppingListId++;
        // Check if the provider of the shopping list is UNKNOWN. If true, set movable false.
        final boolean isProviderUnknownOrFailover = skippedEntities.containsKey(providerOid) &&
                (skippedEntities.get(providerOid).getEntityState() == TopologyDTO.EntityState.UNKNOWN
                || skippedEntities.get(providerOid).getEntityState() == EntityState.FAILOVER);
        if (isProviderUnknownOrFailover) {
            logger.debug("Making movable false for shoppingList of entity {} which has provider " +
                "{} in UNKNOWN/FAILOVER state",
                buyer.getDisplayName(), skippedEntities.get(providerOid).getDisplayName());
        }
        boolean isMovable = !isProviderUnknownOrFailover &&
            // Containers cannot move off their ContainerPods
            !(provider != null && provider.getEntityType() == EntityType.CONTAINER_POD_VALUE) &&
            (commBoughtGrouping.hasMovable()
                ? commBoughtGrouping.getMovable()
                : AnalysisUtil.MOVABLE_TYPES.contains(entityType));
        if (TopologyConversionUtils.isVsanStorage(buyer)) {
            isMovable = false;
        }
        if (commBoughtGrouping.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE) {
            // Turn off movable for cloud scaling group members that are not group leaders.
            isMovable &= addGroupFactor && consistentScalingHelper.getGroupFactor(buyer) > 0;
        }
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
        if (addGroupFactor) {
            // Since we added a coupon commodity, the group factor will be required in order to
            // obtain a price quote.  Set it to zero here, just to make the group factor present
            // in the SL. The actual group factor will be set in the scaling group leader after
            // the proper value is known.
            economyShoppingListBuilder
                .setGroupFactor(consistentScalingHelper.getGroupFactor(buyer));
        }
        final Integer providerEntityType = commBoughtGrouping.hasProviderEntityType() ?
                commBoughtGrouping.getProviderEntityType() : null;
        final Long resourceId = commBoughtGrouping.hasVolumeId() ? commBoughtGrouping.getVolumeId() : null;
        shoppingListOidToInfos.put(id,
            new ShoppingListInfo(id, buyerOid, providerOid, resourceId, providerEntityType,
                    commBoughtGrouping.getCommodityBoughtList()));

        if (enableSMA && includeByType(commBoughtGrouping.getProviderEntityType())
                && commBoughtGrouping.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE) {
            cloudVmComputeShoppingListIDs.add(id);
            return economyShoppingListBuilder.setMovable(false).build();
        } else {
            return economyShoppingListBuilder.build();
        }
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
            if (region == null) {
                logger.warn("Region not found for tier {}", marketTier.getDisplayName());
                return Optional.empty();
            }
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
                    .setPeakQuantity(entityOidToDto.get(marketTier.getTier().getOid()).getTypeSpecificInfo()
                            .getComputeTier().getNumCoupons())
                    .setQuantity(couponQuantity).build();
        }
        return Optional.ofNullable(couponCommBought);
    }

    /**
     * Creates template exclusion commodities bought for cloud consumer.
     *
     * @param providerOid oid of the market tier provider oid
     * @param buyerOid the cloud consumer
     * @return the list of template exclusion commodities the cloud consumer needs to buy
     */
    private Set<CommodityBoughtTO> createTierExclusionCommodityBoughtForCloudEntity(
            long providerOid, long buyerOid) {
        MarketTier marketTier = cloudTc.getMarketTier(providerOid);
        int providerEntityType = marketTier.getTier().getEntityType();
        if (TopologyDTOUtil.isPrimaryTierEntityType(providerEntityType)) {
            return tierExcluder.getTierExclusionCommoditiesToBuy(buyerOid).stream()
                .map(ct -> CommodityBoughtTO.newBuilder()
                    .setSpecification(commodityConverter.commoditySpecification(ct))
                    .build())
                .collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }

    /**
     * Creates the BiClique commodity bought if the provider is a compute market tier
     * or a storage market tier.
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
            Optional.ofNullable(dsBasedBicliquer.getBcKeys(String.valueOf(providerOid)))
                    .ifPresent(keys -> keys.stream().filter(Objects::nonNull)
                    .forEach(bcKeys::add));
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
    private Pair<Long, Class> getProviderId(@Nonnull CommoditiesBoughtFromProvider commodityBoughtGrouping,
                               @Nonnull TopologyEntityDTO topologyEntity) {
        Long providerId = commodityBoughtGrouping.hasProviderId() ?
            commodityBoughtGrouping.getProviderId() :
            null;
        if (providerId == null) {
            return new Pair(null, null);
        }
        Class resultType = MarketTier.class;
        // If the provider id is a compute tier / storage tier/ database tier, then it is a
        // cloud VM / DB. In this case, get the marketTier from
        // [tier x region] combination.
        TopologyEntityDTO providerTopologyEntity = entityOidToDto.get(providerId);
        if (providerTopologyEntity != null && (TopologyDTOUtil.isTierEntityType(
                providerTopologyEntity.getEntityType()))) {
            // Provider is a compute tier / storage tier / database tier
            // Get the region connected to the topologyEntity
            Optional<EntityReservedInstanceCoverage> coverage = cloudTc
                    .getRiCoverageForEntity(topologyEntity.getOid());

            TopologyEntityDTO region = cloudTc.getRegionOfCloudConsumer(topologyEntity);
            if (providerTopologyEntity.getEntityType() == EntityType.COMPUTE_TIER_VALUE &&
                    coverage.isPresent() && region != null &&
                    !coverage.get().getCouponsCoveredByRiMap().isEmpty()) {
                long riId = coverage.get().getCouponsCoveredByRiMap().keySet().iterator().next();
                final ReservedInstanceData riData = cloudTc.getRiDataById(riId);
                final TopologyEntityDTO computeTier =
                        entityOidToDto.get(riData.getReservedInstanceSpec()
                                .getReservedInstanceSpecInfo().getTierId());
                final long businessAccountId = riData.getReservedInstanceBought()
                        .getReservedInstanceBoughtInfo().getBusinessAccountId();
                final Optional<GroupAndMembers> billingFamilyGroup = cloudTopology
                        .getBillingFamilyForEntity(businessAccountId);
                if (!billingFamilyGroup.isPresent()) {
                    logger.error("Billing family group not found for RI Data: {}",
                            riData.getReservedInstanceBought());
                }
                final long billingFamilyId = billingFamilyGroup.map(group ->
                        group.group().getId()).orElse(businessAccountId);
                final ReservedInstanceKey riKey = new ReservedInstanceKey(riData,
                        computeTier.getTypeSpecificInfo().getComputeTier().getFamily(),
                        billingFamilyId);
                final ReservedInstanceAggregate riAggregate =
                        new ReservedInstanceAggregate(riData, riKey, computeTier,
                                Collections.emptySet());
                providerId = cloudTc.getTraderTOOid(new RiDiscountedMarketTier(computeTier,
                        region, riAggregate));
                resultType = RiDiscountedMarketTier.class;
            } else {
                providerId = cloudTc.getTraderTOOid(new OnDemandMarketTier(
                        providerTopologyEntity));
                resultType = OnDemandMarketTier.class;
            }
        }
        return new Pair<>(providerId, resultType);
    }

    private CommodityDTOs.CommodityBoughtTO convertCommodityBought(
            final TopologyEntityDTO buyer, @Nonnull final CommodityBoughtDTO topologyCommBought,
            @Nullable final Long providerOid,
            final boolean shopTogether,
            final Map<Long, Long> providers,
            final Optional<ScalingGroupUsage> scalingGroupUsage) {
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
            : createAndValidateCommBoughtTO(buyer, topologyCommBought, providerOid,
                scalingGroupUsage);
    }

    private CommodityDTOs.CommodityBoughtTO createAndValidateCommBoughtTO(
            final TopologyEntityDTO buyer,
            CommodityBoughtDTO topologyCommBought, @Nullable final Long providerOid,
            final Optional<ScalingGroupUsage> scalingGroupUsage) {
        Pair<Double, Double> quantities = getCommBoughtQuantities(buyer, topologyCommBought,
                providerOid, scalingGroupUsage);
        return CommodityDTOs.CommodityBoughtTO.newBuilder()
                .setQuantity(quantities.first.floatValue())
                .setPeakQuantity(quantities.second.floatValue())
                .setSpecification(commodityConverter.commoditySpecification(
                        topologyCommBought.getCommodityType()))
                .build();
    }

    /**
     * Return the used and peak used amounts for the given commodity bought.  If the amounts were
     * previously calculated as part of the top commodity usage computation phase, those values
     * will be used instead.
     * @param buyer buyer
     * @param topologyCommBought commodity to get usage for
     * @param providerOid OID of provider
     * @param scalingGroupUsage pre-calculated scaling group usage, if available.
     * @return a two element array containing the used and peak used values.
     */
    public Pair<Double, Double> getCommBoughtQuantities(
        final TopologyEntityDTO buyer,
        CommodityBoughtDTO topologyCommBought, @Nullable final Long providerOid,
        final Optional<ScalingGroupUsage> scalingGroupUsage) {
        if (scalingGroupUsage.isPresent()) {
            Optional<Double> cachedUsage = scalingGroupUsage.get().getUsageForCommodity(topologyCommBought);
            if (cachedUsage.isPresent()) {
                return new Pair<>(cachedUsage.get(), cachedUsage.get());
            }
        }
        return getCommBoughtQuantities(buyer, topologyCommBought, providerOid);
    }

    private Pair<Double, Double> getCommBoughtQuantities(final TopologyEntityDTO buyer,
                                                         CommodityBoughtDTO topologyCommBought,
                                                         @Nullable final Long providerOid) {
        final double[] newQuantity = getResizedCapacity(buyer, topologyCommBought, providerOid);
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
        usedQuantity *= topologyCommBought.getScalingFactor();

        // Bought Flow-0 commodity must have quantity of 1.
        if (topologyCommBought.getCommodityType().getType() ==
            CommodityDTO.CommodityType.FLOW_VALUE &&
            "FLOW-0".equals(topologyCommBought.getCommodityType().getKey())) {
            usedQuantity = 1D;
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
        return new Pair<>(usedQuantity, peakQuantity);
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

        Optional<CommodityType> commTypeOptional =
                commodityConverter.economyToTopologyCommodity(commSoldTO.getSpecification());
        if (!commTypeOptional.isPresent() ||
            tierExcluder.isCommSpecTypeForTierExclusion(commSoldTO.getSpecification().getType())) {
            return Optional.empty();
        }

        CommodityType commType = commTypeOptional.get();
        TraderTO projectedTraderTO = oidToProjectedTraderTOMap.get(traderOid);

        /* Avoid invoking the getPrimaryMarketTier on On-Prem entities since getPrimaryMarketTier
        assumes the entity is a cloud entity.*/
        long primaryTierSize = projectedTraderTO.getShoppingListsList().stream()
                .filter(sl -> cloudTc.isMarketTier(sl.getSupplier()))
                .count();
        MarketTier marketTier =
                (primaryTierSize > 0) ? cloudTc.getPrimaryMarketTier(projectedTraderTO) : null;
        // find original sold commodity of same type from original entity
        Optional<CommoditySoldDTO> originalCommoditySold =
                commodityIndex.getCommSold(traderOid, commType);
        float usage = commSoldTO.getQuantity();
        float capacity = getCapacityForCommodity(commSoldTO, marketTier, commType, traderOid);
        double newCapacity = reverseScaleComm(capacity, originalCommoditySold, CommoditySoldDTO::getScalingFactor);
        CommoditySoldDTO.Builder commoditySoldBuilder = CommoditySoldDTO.newBuilder()
            .setCapacity(newCapacity)
            .setUsed(reverseScaleComm(usage, originalCommoditySold, CommoditySoldDTO::getScalingFactor))
            .setPeak(peak)
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
        if (originalCommoditySold.isPresent()
                && originalCommoditySold.get().hasHistoricalUsed()
                && originalCommoditySold.get().getHistoricalUsed().hasPercentile()) {
            float existingPercentile = (float)originalCommoditySold.get().getHistoricalUsed()
                                        .getPercentile();
            double existingCapacity = originalCommoditySold.get().getCapacity();
            float projectedPercentile = (float)(existingCapacity / newCapacity) * existingPercentile;
            commoditySoldBuilder.setHistoricalUsed(HistoricalValues.newBuilder()
                    .setPercentile(projectedPercentile)
                    .build());
        }

        return Optional.of(commoditySoldBuilder.build());
    }

    /**
     * Find the capacity of the old provider for a given sold commodity TO.
     * @param commSoldTO the soldTO for which we want the new capacity.
     * @param marketTier the current market tier for the trader selling the commodity.
     * @param commType the commodity type.
     * @param traderOid the trader OID.
     * @return the new capacity for the passed sold TO.
     */
    private float getCapacityForCommodity(final CommoditySoldTO commSoldTO,
                                          final MarketTier marketTier,
                                          final CommodityType commType,
                                          final long traderOid) {
        float capacity = commSoldTO.getCapacity();

        // If it is a tier based cloud sold TO, return the new provider capacity
        if (marketTier != null && marketTier.getTier() != null) {
            /* We don't need to calculate the projected capacity of non-resizable commodities.
            For instance, the vStorage commodity of an AWS RDB has the capacity as the allocated
            storage for that DBS and Turbo does not scale storage in RDS. Use the resizable flag
            from the original DTO rather than the TO since the resizable
            flag is overridden in the commodity TO for all cloud commodity TOS.*/
            Optional<CommoditySoldDTO> soldDTO = commodityIndex.getCommSold(traderOid, commType);
            if (soldDTO.isPresent() && !soldDTO.get().getIsResizeable()) {
                return  capacity;
            }
            /*
             The capacity of the sold commodity in the projected TO needs to be
             updated to the new provider (cloud tier) capacity.
             */
            final TopologyEntityDTO tier = marketTier.getTier();
            final int tierType = tier.getEntityType();
            Map<Integer, Integer> commTypeMap = null;
            /* Check if there is a corresponding commodity sold by a cloud tier
            for this bought commodity. For example, a vMem commodity sold by a VM
            corresponds to a mem commodity sold by a compute tier.
            */
             commTypeMap =
                    TopologyConversionConstants.entityCommTypeToTierCommType.get(tierType);
            int commTypeValue = commType.getType();
            final int tierComodityType =
                    (commTypeMap != null && commTypeMap.containsKey(commTypeValue)) ?
                            commTypeMap.get(commTypeValue) : commTypeValue;
            Optional<CommoditySoldDTO> tierSoldDTO = marketTier.getTier()
                    .getCommoditySoldListList().stream().filter(commoditySoldDTO ->
                            commoditySoldDTO.getCommodityType().getType() == tierComodityType).findFirst();
            if (tierSoldDTO.isPresent()) {
                return (float)tierSoldDTO.get().getCapacity();
            } else {
                logger.warn("Could not determine new provider capacity for sold commodity" +
                        " of type {} for trader {}.Using usage as received from trader from market.",
                        commType.getType(), traderOid);
            }
        } else if (commSoldTO.getSpecification().getBaseType() == CommodityDTO.CommodityType.VCPU_VALUE) {
            capacity = calculateVCPUResizeCapacityForVM(traderOid, commSoldTO);
        }
        return capacity;
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
                capacity = (float)Math.ceil(capacity / cpuCoreMhz) * cpuCoreMhz;
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
     * If this commodity in the original {@link TopologyEntityDTO} in the input topology
     * had a scale factor, then reverse the scaling on the way out. In other words, since
     * we multiplied by the scale factor on the way in to the market analysis, here we
     * divide by the scale factor on the way back out.
     * <p/>
     * Note that if there is a scale factor but that scale factor is zero, which should never happen,
     * we simply return the originalValue, i.e. we do _not_ return Inf or NaN.
     *
     * @param <T> type of the commodity which value will be scaled.
     * @param valueToReverseScale the commodity value output from the market to
     *                            scale down (if there is a scaleFactor)
     * @param commodity commodity which might have unusual scaling factor
     * @param scalingFactorExtractor function to extract scaling factor from commodity.
     * @return either the valueToReverseScale divided by the scaleFactor if the original
     * commodity had defined a scaleFactor, else the valueToReverseScale unmodified
     */
    private <T> double reverseScaleComm(final double valueToReverseScale,
                    @Nonnull final Optional<T> commodity,
                    @Nonnull Function<T, Double> scalingFactorExtractor) {
        return commodity.map(scalingFactorExtractor).filter(sf -> {
            // Scaling factor should be positive, and not an infinitely small value.
            return (sf - EPSILON) > 0;
        }).map(sf -> valueToReverseScale / sf).orElse(valueToReverseScale);
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

    /**
     * Adds the provided RI coverage to the projected RI coverage.
     * @param entityBuyRICoverage The RI coverage to add, formatted as
     * {@literal <Entity OID, RI ID, Coverage Amount>}
     */
    public void addBuyRICoverageToProjectedRICoverage(@Nonnull Table<Long, Long, Double> entityBuyRICoverage) {
        entityBuyRICoverage.rowMap().forEach((entityOid, buyRICoverage) -> {
            if (projectedReservedInstanceCoverage.containsKey(entityOid)) {
                final EntityReservedInstanceCoverage entityRICoverage =
                        projectedReservedInstanceCoverage.get(entityOid);

                projectedReservedInstanceCoverage.put(entityOid,
                        entityRICoverage.toBuilder()
                                .putAllCouponsCoveredByBuyRi(buyRICoverage)
                                .build());
                logger.debug("Updated projected RI coverage for entity with buy RI coverage" +
                        "(Entity OID={}, Buy RI coverage={})",
                        () -> entityOid,
                        () -> Joiner.on(", ")
                                .withKeyValueSeparator("=>")
                                .join(buyRICoverage));
            } else {
                // It is expected that calculateProjectedRiCoverage() will add a record
                // for every entity in the projected topology
                logger.error("Projected RI coverage for entity could not be found in adding buy RI coverage" +
                        "(Entity OID={})", entityOid);
            }
        });
    }

    /**
     * Given TraderTOs to be sent to market remove skipped entities from this set.
     * @param traderTOs set of TraderTOs sent to market.
     */
    public void removeSkippedEntitiesFromTraderTOs(Set<TraderTO> traderTOs) {
        // Remove all skipped traders as we don't want to send them to market
        // and only needed them for scoping.
        for (long skippedEntityOid : skippedEntities.keySet()) {
            traderTOs.remove(oidToOriginalTraderTOMap.get(skippedEntityOid));
        }
    }

    /**
     * Find intersection between skipped entities and entities in scope to return
     * entities in scope but were skipped.
     * @param scopeEntitiesSet set of entities in scope.
     * @return set of entities that were skipped but were in scope.
     */
    public Set<TopologyEntityDTO> getSkippedEntitiesInScope(Set<Long> scopeEntitiesSet) {
        // Filtering is required only when it is a plan
        // as scoping is possible only in plans.
        if (isPlan()) {
            return skippedEntities.keySet().stream()
                .filter(oid -> scopeEntitiesSet.contains(oid))
                .map(oid -> skippedEntities.get(oid))
                .collect(Collectors.toSet());
        } else {
            // For realtime we should return all skipped entities.
            return skippedEntities.values().stream().collect(Collectors.toSet());
        }
    }

    /**
     * Return a CommSpecTO for the CommodityType passed.
     * @param commType is the commType we need converted
     * @return {@link com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO}
     * equivalent of the commType.
     */
    public CommodityDTOs.CommoditySpecificationTO getCommSpecForCommodity(CommodityType commType) {
        return commodityConverter.commoditySpecification(commType);
    }

    /**
     * Return the consistent scaling helper associated with this TC.
     * @return consistent scaling helper
     */
    public ConsistentScalingHelper getConsistentScalingHelper() {
        return consistentScalingHelper;
    }
}
