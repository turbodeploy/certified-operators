package com.vmturbo.market.topology.conversions;

import static com.vmturbo.commons.analysis.RawMaterialsMap.rawMaterialsMap;
import static com.vmturbo.market.topology.conversions.MarketAnalysisUtils.ACCESS_COMMODITY_TYPES;
import static com.vmturbo.market.topology.conversions.TopologyConversionUtils.CLOUD_VOLUME_COMMODITIES_UNIT_CONVERSION;
import static com.vmturbo.market.topology.conversions.TopologyConversionUtils.calculateFactorForCommodityValues;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Enums;
import com.google.common.base.Suppliers;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.math.DoubleMath;
import com.google.gson.Gson;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.springframework.util.CollectionUtils;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.TypeCase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.commons.analysis.RawMaterialsMap.RawMaterialInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.market.AnalysisRICoverageListener;
import com.vmturbo.market.runner.Analysis;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.FakeEntityCreator;
import com.vmturbo.market.runner.MarketMode;
import com.vmturbo.market.runner.reservedcapacity.ReservedCapacityResults;
import com.vmturbo.market.runner.wasted.WastedEntityResults;
import com.vmturbo.market.runner.wasted.files.WastedFilesResults;
import com.vmturbo.market.settings.EntitySettings;
import com.vmturbo.market.settings.MarketSettings;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.RiDiscountedMarketTier;
import com.vmturbo.market.topology.SingleRegionMarketTier;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.market.topology.conversions.CommoditiesResizeTracker.CommodityLookupType;
import com.vmturbo.market.topology.conversions.CommodityIndex.CommodityIndexFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ScalingGroup;
import com.vmturbo.market.topology.conversions.ConversionErrorCounts.ErrorCategory;
import com.vmturbo.market.topology.conversions.ConversionErrorCounts.Phase;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator;
import com.vmturbo.mediation.hybrid.cloud.utils.StorageTier;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
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
import com.vmturbo.platform.common.dto.CommonDTO.CommodityCapacityLimit;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Convert topology DTOs to economy DTOs.
 */
public class TopologyConverter {
    private static final Logger logger = LogManager.getLogger();

    public static final Set<TopologyDTO.EntityState> SKIPPED_ENTITY_STATES = ImmutableSet.of(
            TopologyDTO.EntityState.UNKNOWN,
            TopologyDTO.EntityState.MAINTENANCE,
            TopologyDTO.EntityState.FAILOVER);

    private enum LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY {
        PM_NOT_CONTROLLABLE("PM %s is not controllable"),
        PM_SKIPPED("PM %s is skipped"),

        VM_NOT_MOVABLE("VM %s is not movable for provider %s"),
        VM_NOT_CONTROLLABLE("VM %s is not controllable"),
        VM_SKIPPED("VM %s is skipped"),

        STORAGE_NOT_CONTROLLABLE("Storage %s is not controllable"),
        STORAGE_SKIPPED("Storage %s is skipped");

        private final String message;

        LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY(@NonNull final String format) {
            final String prefix = "Uncontrollable entity: ";
            this.message = prefix + format;
        }

        static String formatEntity(@Nullable TopologyEntityDTO entity){
            return (entity != null) ? "'" + entity.getDisplayName() + "' (oid=" + entity.getOid() + ")" : null;
        }

        public Supplier<String> format(@NonNull final TopologyEntityDTO entity, @Nullable TopologyEntityDTO relatedEntity) {
            Object[] formatArgs;
            final String entityPrint = formatEntity(entity);
            final String relatedEntityPrint = formatEntity(relatedEntity);
            formatArgs = new Object[]{entityPrint, relatedEntityPrint};
            return () -> String.format(message, formatArgs);
        }
    }

    private void logUncontrollable(@NonNull LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY msg,
            @Nonnull TopologyEntityDTO entity, @Nullable TopologyEntityDTO relatedEntity) {

        Supplier<String> message = msg.format(entity, relatedEntity);
        if (isPlan()) {
            logger.info(message::get);
            return;
        }
        logger.trace(message::get);
    }

    static final ImmutableMap<Integer, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY>
            uncontrollableEntityToMessage =
            ImmutableMap.<Integer, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY>builder()
                    .put(EntityType.PHYSICAL_MACHINE_VALUE, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY.PM_NOT_CONTROLLABLE)
                    .put(EntityType.VIRTUAL_MACHINE_VALUE, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY.VM_NOT_CONTROLLABLE)
                    .put(EntityType.STORAGE_VALUE, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY.STORAGE_NOT_CONTROLLABLE)
                    .build();

    static final ImmutableMap<Integer, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY>
            unmovableEntityToMessage =
            ImmutableMap.<Integer, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY>builder()
                    .put(EntityType.VIRTUAL_MACHINE_VALUE, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY.VM_NOT_MOVABLE)
                    .build();

    static final ImmutableMap<Integer, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY>
            skippedEntityToMessage =
            ImmutableMap.<Integer, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY>builder()
                    .put(EntityType.PHYSICAL_MACHINE_VALUE, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY.PM_SKIPPED)
                    .put(EntityType.VIRTUAL_MACHINE_VALUE, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY.VM_SKIPPED)
                    .put(EntityType.STORAGE_VALUE, LOG_MESSAGE_FOR_UNCONTROLLABLE_ENTITY.STORAGE_SKIPPED)
                    .build();

    public static final boolean INCLUDE_GUARANTEED_BUYER_DEFAULT =
            MarketSettings.BooleanKey.INCLUDE_GUARANTEED_BUYER.value();

    // Key is the type of original entity, value is a set of types
    // Copy the connected entities of original entity into the projected entity except for the
    // ones in the map, which need to be computed like Availability zone or Region
    // because the AZ or Region might have changed.
    private final Map<Integer, Set<Integer>> projectedConnectedEntityTypesToCompute = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE,
                    ImmutableSet.of(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE,
                                    // connections of vm to volume are added during extra resources creation
                                    EntityType.VIRTUAL_VOLUME_VALUE),
            EntityType.DATABASE_VALUE, ImmutableSet.of(EntityType.AVAILABILITY_ZONE_VALUE),
            EntityType.DATABASE_SERVER_VALUE, ImmutableSet.of(EntityType.AVAILABILITY_ZONE_VALUE));

    private static final Set<Integer> CONTAINER_TYPES = ImmutableSet.of(
        // TODO: Add container collection
        EntityType.CONTAINER_VALUE,
        EntityType.CONTAINER_POD_VALUE);

    // If a cloud entity buys from this set of cloud entity types, then we need to create DataCenter
    // commodity bought for it.
    private static final Set<Integer> CLOUD_ENTITY_TYPES_TO_CREATE_DC_COMM_BOUGHT = ImmutableSet.of(
        EntityType.COMPUTE_TIER_VALUE,
        EntityType.DATABASE_TIER_VALUE,
        EntityType.DATABASE_SERVER_TIER_VALUE
    );

    /**
     * Entity types for which we generate SCALE type actions in the cloud.
     */
    private static final Set<Integer> CLOUD_SCALING_ENTITY_TYPES = ImmutableSet.of(
        EntityType.VIRTUAL_MACHINE_VALUE,
        EntityType.VIRTUAL_VOLUME_VALUE,
        EntityType.DATABASE_VALUE,
        EntityType.DATABASE_SERVER_VALUE,
        EntityType.VIRTUAL_MACHINE_SPEC_VALUE
    );

    private static final BiPredicate<Integer, Integer> IS_CLOUD_VM = (providerType, entityType) -> {
        Map<Integer, Collection<Integer>> entityTypeInScalingGroup =
                ImmutableMap.of(EntityType.COMPUTE_TIER_VALUE,
                        ImmutableSet.of(EntityType.VIRTUAL_MACHINE_VALUE));
        return entityTypeInScalingGroup.containsKey(providerType)
                && entityTypeInScalingGroup.get(providerType).contains(entityType);
    };

    /**
     * A map from the entity type to the commodities. Turbonomic should preserve the old capacities
     * of these commodities.
     */
    private static final Map<Integer, Set<Integer>> OLD_CAPACITY_REQUIRED_ENTITIES_TO_COMMODITIES =
            new HashMap<Integer, Set<Integer>>() {{
                put(EntityType.VIRTUAL_VOLUME_VALUE,
                        ImmutableSet.of(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
                                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE,
                                CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE));
                put(EntityType.DATABASE_VALUE,
                        ImmutableSet.of(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE));
                put(EntityType.DATABASE_SERVER_VALUE,
                        ImmutableSet.of(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
                                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE));
            }};

    private static final double MINIMUM_ACHIEVABLE_IOPS_PERCENTAGE = 0.05;

    /**
     * Provider utilization should be updated when removing the following entities.
     */
    private static final Set<Integer> WORKLOAD_ENTITY_TYPES = ImmutableSet.of(
        EntityType.VIRTUAL_MACHINE_VALUE,
        EntityType.CONTAINER_VALUE,
        EntityType.CONTAINER_POD_VALUE,
        EntityType.APPLICATION_COMPONENT_VALUE);

    private static final Set<Integer> OLD_QUANTITY_REQUIRED_COMM_TYPES = ImmutableSet.of(
            CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
            CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE,
            CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE
    );

    //<CONSUMER_TYPE, SELLER_TYPE, COMMODITY_TYPE_THAT_MAY_HAVE_RESERVATION>
    private static final Map<Integer, Map<Integer, Set<Integer>>> ENTITIES_AND_COMMODITIES_NEEDS_TO_CAP_RESERVATION = ImmutableMap
            .of(EntityType.VIRTUAL_MACHINE_VALUE,
                    ImmutableMap.of(EntityType.PHYSICAL_MACHINE_VALUE, ImmutableSet.of(CommodityDTO.CommodityType.CPU_VALUE, CommodityDTO.CommodityType.MEM_VALUE)));

    // NOTE: This set does not include VIRTUAL_VOLUME because those entities are already getting skipped through
    // TopologyConversionConstants.ENTITY_TYPES_TO_SKIP_TRADER_CREATION.
    private static final Set<Integer> ENTITY_TYPES_WITH_DELETE_ACTIONS = ImmutableSet.of(
            EntityType.VIRTUAL_MACHINE_SPEC_VALUE);


    //Store the entities whose commodity usage need to be modified because of reservation during convertingToMarket
    //<buyerId, commBought, providerId>
    private final Map<Long, Map<CommodityBoughtDTO, Long>> commoditiesWithReservationGreaterThanUsed = Maps.newHashMap();

    // Update projected provider sold value with buyer reserved value after market analysis
    // Map<providerOid, Map<the commodity type to update, the value to be subtracted from sold value>>
    private final Map<Long, Map<CommodityType, Double>> projectedProviderUsedSubtractionMap = Maps.newHashMap();

    private static final float MEDIUM_RATE_OF_RESIZE = 2.0f;

    private static final ImmutableMap<Float, Float> rateOfResizeTranslationMap = ImmutableMap.of(
        // Low rateOfResize
        1.0f, 10000000000.0f,
        // Medium rateOfResize
        MEDIUM_RATE_OF_RESIZE, 4.0f,
        // High rateOfResize
        3.0f, 1.0f);

    // TODO: In legacy this is taken from LicenseManager and is currently false
    private boolean includeGuaranteedBuyer = INCLUDE_GUARANTEED_BUYER_DEFAULT;

    private CloudTopologyConverter cloudTc;

    private final ProjectedRICoverageCalculator projectedRICoverageCalculator;

    private Status costNotificationStatus = Status.UNKNOWN;

    private final Gson gson = new Gson();

    /**
     * Entities that are providers of containers.
     */
    private final Set<Long> providersOfContainers = Sets.newHashSet();

    // Set of oids of compute shopping list of the cloud vm. one entry per cloud vm.
    private final Set<Long> cloudVmComputeShoppingListIDs = new HashSet<>();

    // Store skipped service entities which need to be added back to projected topology and price
    // index messages.
    private final Map<Long, TopologyEntityDTO> skippedEntities = Maps.newHashMap();

    private long shoppingListId = 1000L; // Arbitrary start value

    private final AtomicLong cloneIndex = new AtomicLong(0);

    // used in double comparision
    public static final double EPSILON = 1e-5;

    /**
     * Map from entity OID to original topology entity DTO.
     */
    private final Map<Long, TopologyEntityDTO>
            entityOidToDto = Maps.newHashMap();

    private final Map<Long, TopologyEntityDTO> unmodifiableEntityOidToDtoMap
            = Collections.unmodifiableMap(entityOidToDto);

    // a map to keep the oid to projected traderTO mapping
    private final Map<Long, EconomyDTOs.TraderTO> oidToProjectedTraderTOMap = Maps.newHashMap();

    // a map to keep the oid to original traderTO mapping
    private final Long2ObjectOpenHashMap<MinimalOriginalTrader> oidToOriginalTraderTOMap = new Long2ObjectOpenHashMap<>();

    // Bicliquer created based on datastore
    private final BiCliquer dsBasedBicliquer = new BiCliquer();
    // Bicliquer created based on pm
    private final BiCliquer pmBasedBicliquer = new BiCliquer();

    // Table that stores the number of consumers of a commodity sold by a provider
    private final Table<Long, CommodityType, Integer> numConsumersOfSoldCommTable =
            HashBasedTable.create();
    // Map from bcKey to commodity bought
    private final Map<String, CommodityDTOs.CommodityBoughtTO> bcCommodityBoughtMap = Maps.newHashMap();
    // a BiMap from DSPMAccess and Datastore commodity sold key to seller oid
    // Note: the commodity key is composed of entity type and entity ID (which is different from
    // OID)
    private final BiMap<String, Long> accessesByKey = HashBiMap.create();

    private final Map<TopologyEntityDTO, TopologyEntityDTO> azToRegionMap = new HashMap<>();

    // This map will hold VM/DB -> BusinessAccount mapping.
    private final Map<TopologyEntityDTO, TopologyEntityDTO> cloudEntityToBusinessAccount = new HashMap<>();
    // This will hold the set of all business accounts in the topology
    private final Set<TopologyEntityDTO> businessAccounts = new HashSet<>();

    private final TopologyInfo topologyInfo;

    private final CommoditiesResizeTracker commoditiesResizeTracker = new CommoditiesResizeTracker();

    // a map keeps shoppinglist oid to ShoppingListInfo which is a container for
    // shoppinglist oid, buyer oid, seller oid and commodity bought
    private final Map<Long, ShoppingListInfo> shoppingListOidToInfos = Maps.newHashMap();

    private float quoteFactor = MarketAnalysisUtils.QUOTE_FACTOR;
    private float liveMarketMoveCostFactor = MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR;
    private float storageMoveCostFactor = MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR;

    // Add a cost of moving from source to destination.
    private static final float PLAN_MOVE_COST_FACTOR = 0.0f;
    private static final float CLOUD_QUOTE_FACTOR = 1;

    private final CommodityConverter commodityConverter;

    private final Supplier<ActionInterpreter> actionInterpreter;

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    /**
     * Utility to track errors encountered during conversion.
     */
    private final ConversionErrorCounts conversionErrorCounts = new ConversionErrorCounts();

    /**
     * Index that keeps scaling factors applied during conversion TO market entities, to allow
     * quick lookups to reverse scaling when converting FROM market entities.
     * <p/>
     * Lazily initialized to reduce memory usage in the component until we actually need to
     * use the CommodityIndex when we convert back from market.
     */
    private final Supplier<CommodityIndex> commodityIndex;

    private final TierExcluder tierExcluder;

    private final ConsistentScalingHelper consistentScalingHelper;

    private final MarketMode marketMode;

    private final ReversibilitySettingFetcher reversibilitySettingFetcher;

    private Set<Long> entityOidsWithReversibilityPreferred;

    /**
     * Whether it is a cloud migration plan.
     */
    private boolean isCloudMigration;

    /**
     * Whether resize is enabled, for cloud migration plan.
     */
    private boolean isCloudResizeEnabled;

    /**
     * Whether unquoted commodities are allowed in the market.
     */
    private boolean unquotedCommoditiesEnabled = false;

    /**
     * If the setting is true, we use max(reservation, used)as VM's commodity bought used.
     */
    private boolean useVMReservationAsUsed = false;

    /**
     * Enabling specific logic for single vm on host.
     */
    private boolean singleVMonHost = false;

    /**
     * A utilization threshold that can be used for custom logic.
     */
    private float customUtilizationThreshold;

    private FakeEntityCreator fakeEntityCreator;
    
    private boolean enableOP;
    
    private Collection<WastedEntityResults> wastedEntityResults = new HashSet<>();

    /**
     * Constructor with includeGuaranteedBuyer parameter. Entry point from Analysis.
     *
     * @param topologyInfo Information about the topology.
     * @param includeGuaranteedBuyer whether to include guaranteed buyers (VDC, VPod, DPod) or not
     * @param quoteFactor to be used by move recommendations.
     * @param marketMode the market generates compute scaling action for could vms if false.
     *                  the SMA (Stable Marriage Algorithm)  library generates them if true.
     * @param liveMarketMoveCostFactor used by the live market to control aggressiveness of move actions.
     * @param storageMoveCostFactor used to normalize cost of storage moves.
     * @param marketCloudRateExtractor market price table
     * @param incomingCommodityConverter the commodity converter
     * @param cloudCostData cloud cost data
     * @param commodityIndexFactory commodity index factory
     * @param tierExcluderFactory tierExcluderFactory
     * @param consistentScalingHelperFactory CSM helper factory
     * @param cloudTopology instance to look up topology relationships
     * @param reversibilitySettingFetcher fetcher for "Savings vs Reversibility" policy settings
     * @param licensePriceWeightScale value to scale the price weight of commodities for every
     *            softwareLicenseCommodity sold by a provider.
     * @param enableOP flag to check if to use over provisioning commodity changes.
     * @param singleVMonHost Enabling specific logic for single vm on host.
     * @param customUtilizationThreshold A utilization threshold that can be used for custom logic.
     */
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             final boolean includeGuaranteedBuyer,
                             final float quoteFactor,
                             final MarketMode marketMode,
                             final float liveMarketMoveCostFactor,
                             final float storageMoveCostFactor,
                             @Nonnull final CloudRateExtractor marketCloudRateExtractor,
                             CommodityConverter incomingCommodityConverter,
                             final CloudCostData cloudCostData,
                             final CommodityIndexFactory commodityIndexFactory,
                             @Nonnull final TierExcluderFactory tierExcluderFactory,
                             @Nonnull final ConsistentScalingHelperFactory consistentScalingHelperFactory,
                             @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology,
                             @Nonnull final ReversibilitySettingFetcher reversibilitySettingFetcher,
                             final int licensePriceWeightScale,
                             final boolean enableOP,
                             final boolean useVMReservationAsUsed,
                             final boolean singleVMonHost,
                             final float customUtilizationThreshold,
                             final FakeEntityCreator fakeEntityCreator) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.cloudTopology = cloudTopology;
        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
        this.quoteFactor = quoteFactor;
        this.marketMode = marketMode;
        this.liveMarketMoveCostFactor = liveMarketMoveCostFactor;
        this.storageMoveCostFactor = storageMoveCostFactor;
        this.consistentScalingHelper = consistentScalingHelperFactory
                .newConsistentScalingHelper(topologyInfo, getShoppingListOidToInfos());
        this.commodityConverter = incomingCommodityConverter != null ?
                incomingCommodityConverter : new CommodityConverter(new NumericIDAllocator(),
                includeGuaranteedBuyer, dsBasedBicliquer, numConsumersOfSoldCommTable,
                conversionErrorCounts, consistentScalingHelper, licensePriceWeightScale, enableOP, topologyInfo);
        this.tierExcluder = tierExcluderFactory.newExcluder(topologyInfo, this.commodityConverter,
                getShoppingListOidToInfos());
        this.cloudTc = new CloudTopologyConverter(unmodifiableEntityOidToDtoMap, topologyInfo,
                pmBasedBicliquer, dsBasedBicliquer, this.commodityConverter, azToRegionMap, businessAccounts,
                marketCloudRateExtractor, cloudCostData, tierExcluder, cloudTopology);
        this.commodityIndex = Suppliers.memoize(() -> this.createCommodityIndex(commodityIndexFactory));
        this.projectedRICoverageCalculator = new ProjectedRICoverageCalculator(
            oidToOriginalTraderTOMap, cloudTc, this.commodityConverter);
        this.fakeEntityCreator = fakeEntityCreator;
        this.actionInterpreter = Suppliers.memoize(() -> new ActionInterpreter(
                commodityConverter,
                shoppingListOidToInfos,
                cloudTc,
                unmodifiableEntityOidToDtoMap,
                oidToProjectedTraderTOMap,
                commoditiesResizeTracker,
                projectedRICoverageCalculator,
                tierExcluder,
                commodityIndex,
                getExplanationOverride(),
                Collections.unmodifiableMap(commoditiesWithReservationGreaterThanUsed), fakeEntityCreator)
        );
        this.isCloudMigration = TopologyDTOUtil.isCloudMigrationPlan(topologyInfo);
        this.isCloudResizeEnabled = TopologyDTOUtil.isResizableCloudMigrationPlan(topologyInfo);
        this.reversibilitySettingFetcher = reversibilitySettingFetcher;
        this.useVMReservationAsUsed = useVMReservationAsUsed;
        this.singleVMonHost = singleVMonHost;
        this.customUtilizationThreshold = customUtilizationThreshold;
        this.enableOP = enableOP;
    }

    /**
     * Constructor with analysisConfig parameter. Entry point from Analysis.
     * TODO: consider using a builder pattern since the constructors have many parameters.
     *
     * @param topologyInfo Information about the topology.
     * @param marketCloudRateExtractor market price table
     * @param incomingCommodityConverter the commodity converter
     * @param cloudCostData cloud cost data
     * @param commodityIndexFactory commodity index factory
     * @param tierExcluderFactory tierExcluderFactory
     * @param consistentScalingHelperFactory CSM helper factory
     * @param cloudTopology instance to look up topology relationships
     * @param reversibilitySettingFetcher fetcher for "Savings vs Reversibility" policy settings
     * @param analysisConfig the market config, which contains the market global settings.
     */
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             @Nonnull final CloudRateExtractor marketCloudRateExtractor,
                             CommodityConverter incomingCommodityConverter,
                             final CloudCostData cloudCostData,
                             final CommodityIndexFactory commodityIndexFactory,
                             @Nonnull final TierExcluderFactory tierExcluderFactory,
                             @Nonnull final ConsistentScalingHelperFactory consistentScalingHelperFactory,
                             @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology,
                             @Nonnull final ReversibilitySettingFetcher reversibilitySettingFetcher,
                             @Nonnull final AnalysisConfig analysisConfig,
                             @Nonnull final FakeEntityCreator fakeEntityCreator) {
        this(topologyInfo, analysisConfig.getIncludeVdc(), analysisConfig.getQuoteFactor(),
                analysisConfig.getMarketMode(), analysisConfig.getLiveMarketMoveCostFactor(),
                analysisConfig.getStorageMoveCostFactor(), marketCloudRateExtractor, 
                incomingCommodityConverter, cloudCostData,
                commodityIndexFactory, tierExcluderFactory, consistentScalingHelperFactory,
                cloudTopology, reversibilitySettingFetcher, analysisConfig.getLicensePriceWeightScale(),
                analysisConfig.isEnableOP(), analysisConfig.useVMReservationAsUsed(),
                analysisConfig.isSingleVMonHost(), analysisConfig.getCustomUtilizationThreshold(), fakeEntityCreator);
        this.unquotedCommoditiesEnabled = isUnquotedCommoditiesEnabled(analysisConfig);
    }

    /**
     * is the market mode SMA.
     * @return true if the market mode is SMAOnly or SMALite
     */
    private boolean isSMAOnly() {
        return (marketMode == MarketMode.SMALite
                || marketMode == MarketMode.SMAOnly);
    }

    /**
     * get the TopologyEntityDTO OID corresponding to the oid of a On-demand TemplateProvider.
     * return empty if the traderTOOID is a CBTP.
     * @param marketTier the TemplateProvider.
     * @return the OID of corresponding TopologyEntityDTO
     */
    public Optional<Long> getTopologyEntityOIDForOnDemandMarketTier(MarketTier marketTier) {
        if (marketTier.hasRIDiscount()) {
            return Optional.empty();
        } else {
            return Optional.of(marketTier.getTier().getOid());
        }
    }

    private boolean isReversibilityPreferred(final long entityOid) {
        if (entityOidsWithReversibilityPreferred == null) {
            entityOidsWithReversibilityPreferred = reversibilitySettingFetcher
                    .getEntityOidsWithReversibilityPreferred();
        }
        return entityOidsWithReversibilityPreferred.contains(entityOid);
    }

    public Map<Long, TopologyEntityDTO> getUnmodifiableEntityOidToDtoMap() {
        return unmodifiableEntityOidToDtoMap;
    }

    public Map<Long, MinimalOriginalTrader> getUnmodifiableOidToOriginalTraderTOMap() {
        return Collections.unmodifiableMap(oidToOriginalTraderTOMap);
    }

    @VisibleForTesting
    protected Map<Long, TopologyEntityDTO> getSkippedEntities() {
        return skippedEntities;
    }

    public Set<Long> getCloudVmComputeShoppingListIDs() {
        return cloudVmComputeShoppingListIDs;
    }

    public ProjectedRICoverageCalculator getProjectedRICoverageCalculator() {
        return projectedRICoverageCalculator;
    }

    public CommodityIndex getCommodityIndex() {
        return commodityIndex.get();
    }

    public CloudTopologyConverter getCloudTc() {
        return cloudTc;
    }

    @VisibleForTesting
    protected void setCloudTc(final CloudTopologyConverter cloudTc) {
        this.cloudTc = cloudTc;
    }

    // Read only version
    public final Map<Long, ShoppingListInfo> getShoppingListOidToInfos() {
        return Collections.unmodifiableMap(shoppingListOidToInfos);
    }

    private boolean isPlan() {
        return TopologyDTOUtil.isPlan(topologyInfo);
    }

    /**
     * Whether we have finished converting to market already.
     */
    private boolean convertToMarketComplete = false;

    /**
     * Convert a collection of common protobuf topology entity DTOs to analysis protobuf economy DTOs.
     * @param topology list of topology entity DTOs
     * @return set of economy DTOs
     */
    @Nonnull
    public Collection<EconomyDTOs.TraderTO> convertToMarket(
                @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> topology) {
        return convertToMarket(topology, Collections.emptySet());
    }

    /**
     * Convert a collection of common protobuf topology entity DTOs to analysis protobuf economy DTOs.
     * @param topology list of topology entity DTOs
     * @param oidsToRemove oids to remove
     * @return set of economy DTOs
     */
    @Nonnull
    public Collection<EconomyDTOs.TraderTO> convertToMarket(
                @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> topology,
                @Nonnull final Set<Long> oidsToRemove) {
        // Initialize the consistent resizer
        if (!TopologyDTOUtil.isCloudMigrationPlan(topologyInfo)) {
            consistentScalingHelper.initialize(topology);
        }
        // TODO (roman, Jul 5 2018): We don't need to create a new entityOidToDto map.
        // We can have a helper class that will apply the skipped entity logic on the
        // original topology.
        conversionErrorCounts.startPhase(Phase.CONVERT_TO_MARKET);
        long convertToMarketStartTime = System.currentTimeMillis();
        final Set<Long> tierExcluderEntityOids = new HashSet<>();
        final Collection<Long> wastedEntityIds = extractWastedEntityIds(ENTITY_TYPES_WITH_DELETE_ACTIONS);
        final Set<Integer> tierExcluderEntityTypeScope = EntitySettingSpecs.ExcludedTemplates
            .getEntityTypeScope().stream().map(EntityType::getNumber).collect(Collectors.toSet());
        try {
            for (TopologyDTO.TopologyEntityDTO entity : topology.values()) {
                try {
                    final int entityType = entity.getEntityType();
                    if (MarketAnalysisUtils.SKIPPED_ENTITY_TYPES.contains(entityType)
                            || !includeByType(entityType)
                            || wastedEntityIds.contains(entity.getOid())) {
                        logger.debug("Skipping trader creation for entity name = {}, entity type = {}, " +
                                "entity state = {}", entity.getDisplayName(),
                            EntityType.forNumber(entityType), entity.getEntityState());
                        skippedEntities.put(entity.getOid(), entity);
                    } else {
                        // Allow creation of traderTOs for traders discarded due to state or entityType.
                        // We will remove them traderTO set after scoping but before sending to market.
                        if (SKIPPED_ENTITY_STATES.contains(entity.getEntityState())) {
                            logger.debug("Skipping trader creation for entity oid = {}, name = {}, type = {}, "
                                            + "state = {} because of state.", entity.getOid(), entity.getDisplayName(),
                                    EntityType.forNumber(entityType), entity.getEntityState());
                            skippedEntities.put(entity.getOid(), entity);

                            if (skippedEntityToMessage.containsKey(entity.getEntityType())) {
                                logUncontrollable(skippedEntityToMessage.get(entityType),
                                        entity, null);
                            }
                        }
                        entityOidToDto.put(entity.getOid(), entity);
                        if (CONTAINER_TYPES.contains(entityType)) {
                            // ContainerPods, VMs or VirtualVolumes.
                            Set<Long> containerProviderSet = entity.getCommoditiesBoughtFromProvidersList()
                                .stream()
                                .filter(CommoditiesBoughtFromProvider::hasProviderId)
                                .map(CommoditiesBoughtFromProvider::getProviderId)
                                .collect(Collectors.toSet());
                            providersOfContainers.addAll(containerProviderSet);
                            // Add VirtualVolume to skippedEntities if it's provider of a containerPod.
                            // ContainerPods are not allowed to move across volumes.
                            // This is to make sure corresponding containerPods are still placed on
                            // their original volumes after the analysis to avoid incorrect unplacement
                            // results.
                            if (entityType == EntityType.CONTAINER_POD_VALUE) {
                                containerProviderSet.stream()
                                    .map(topology::get)
                                    .filter(providerEntity -> providerEntity != null
                                        && providerEntity.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                                    .forEach(providerEntity -> skippedEntities.put(providerEntity.getOid(), providerEntity));
                            }
                        }
                        if (entityType == EntityType.REGION_VALUE) {
                            List<TopologyEntityDTO> AZs = TopologyDTOUtil.getConnectedEntitiesOfType(
                                entity, EntityType.AVAILABILITY_ZONE_VALUE, topology);
                            AZs.forEach(az -> azToRegionMap.put(az, entity));
                        } else if (entityType == EntityType.BUSINESS_ACCOUNT_VALUE) {
                            List<TopologyEntityDTO> vms = TopologyDTOUtil.getConnectedEntitiesOfType(
                                entity, EntityType.VIRTUAL_MACHINE_VALUE, topology);
                            List<TopologyEntityDTO> dbs = TopologyDTOUtil.getConnectedEntitiesOfType(
                                entity, EntityType.DATABASE_VALUE, topology);
                            List<TopologyEntityDTO> dbss = TopologyDTOUtil.getConnectedEntitiesOfType(
                                entity, EntityType.DATABASE_SERVER_VALUE, topology);
                            List<TopologyEntityDTO> virtualMachineSpecs = TopologyDTOUtil.getConnectedEntitiesOfType(
                                entity, EntityType.VIRTUAL_MACHINE_SPEC_VALUE, topology);
                            vms.forEach(vm -> cloudEntityToBusinessAccount.put(vm, entity));
                            dbs.forEach(db -> cloudEntityToBusinessAccount.put(db, entity));
                            dbss.forEach(db -> cloudEntityToBusinessAccount.put(db, entity));
                            virtualMachineSpecs.forEach(v -> cloudEntityToBusinessAccount.put(v, entity));
                            businessAccounts.add(entity);
                            if (cloudTc.getAccountPricingIdFromBusinessAccount(entity.getOid()).isPresent()) {
                                cloudTc.insertIntoAccountPricingDataByBusinessAccountOidMap(entity.getOid(),
                                        cloudTc.getAccountPricingIdFromBusinessAccount(entity.getOid()).get());
                            }
                        }

                        // Store oids of entities that may have ExcludedTemplates settings.
                        if (entity.getEnvironmentType() == EnvironmentType.CLOUD &&
                            tierExcluderEntityTypeScope.contains(entityType)) {
                            tierExcluderEntityOids.add(entity.getOid());
                        }

                        //If a VM's bought CPU used is smaller than its CPU reservation, we record this commodityBought along with its providerId
                        //While converting to shopping list, we use the reservation of the CPU commodity as its quantity
                        //We also need to add the diff (reservation - used) to sold used of the provider host.
                        if (useVMReservationAsUsed && !oidsToRemove.contains(entity.getOid()) &&
                                entity.getEnvironmentType() == EnvironmentType.ON_PREM &&
                                entity.getEntityState() == EntityState.POWERED_ON &&
                                ENTITIES_AND_COMMODITIES_NEEDS_TO_CAP_RESERVATION.containsKey(entity.getEntityType())) {
                            Map<Integer, Set<Integer>> providerTypesWithReservation =
                                    ENTITIES_AND_COMMODITIES_NEEDS_TO_CAP_RESERVATION.get(entity.getEntityType());
                            for (CommoditiesBoughtFromProvider commoditiesBoughtFromProvider: entity.getCommoditiesBoughtFromProvidersList()) {
                                if (providerTypesWithReservation.containsKey(commoditiesBoughtFromProvider.getProviderEntityType()) &&
                                        topology.get(commoditiesBoughtFromProvider.getProviderId()) != null) {
                                    Set<Integer> commodityTypesWithReservation =
                                            providerTypesWithReservation.get(commoditiesBoughtFromProvider.getProviderEntityType());
                                    for (CommodityBoughtDTO commodity: commoditiesBoughtFromProvider.getCommodityBoughtList()) {
                                        if (commodityTypesWithReservation.contains(commodity.getCommodityType().getType()) &&
                                            commodity.getReservedCapacity() > commodity.getUsed()) {
                                            logger.debug("Adding entity {} 's {} commodity into commoditiesWithReservationGreaterThanUsed table"
                                            , entity.getOid(), commodity.getCommodityType().getType());
                                            commoditiesWithReservationGreaterThanUsed.computeIfAbsent(entity.getOid(), key -> new HashMap<>())
                                                .put(commodity, commoditiesBoughtFromProvider.getProviderId());
                                        }
                                    }
                                }
                            }
                        }

                    }
                } catch (Exception e) {
                    logger.error(EconomyConstants.EXCEPTION_MESSAGE, entityDebugInfo(entity),
                            e.getMessage(), e);
                }
            }

            // Initialize the template exclusion applicator
            tierExcluder.initialize(topology, tierExcluderEntityOids);

            Map<Long, Map<TopologyDTO.CommodityType, UsedAndPeak>> providerUsedModificationMap = createProviderUsedModificationMap(entityOidToDto, oidsToRemove);
            //For bought commodities whose reservation is greater than used, we need to add reservation - used onto its provider.
            commoditiesWithReservationGreaterThanUsed.values().stream()
                .flatMap(map -> map.entrySet().stream())
                .forEach(entry ->
                    addReservationUsedDiffIntoProviderUsedModificationMap(providerUsedModificationMap,
                        entry.getKey(), entry.getValue()));
            commodityConverter.setProviderUsedSubtractionMap(providerUsedModificationMap);

            Collection<TraderTO> convertedTraders = convertToMarket();
            convertedTraders.forEach(t -> oidToOriginalTraderTOMap.put(t.getOid(), new MinimalOriginalTrader(t)));
            oidToOriginalTraderTOMap.trim();
            return convertedTraders;
        } catch (Exception e) {
            logger.error(EconomyConstants.EXCEPTION_MESSAGE,
                "convertToMarket", e.getMessage(), e);
            return new HashSet<>();
        } finally {
            conversionErrorCounts.endPhase();
            tierExcluder.clearStateNeededForConvertToMarket();
            setConvertToMarketComplete();
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
    private Collection<EconomyDTOs.TraderTO> convertToMarket() {
        final Collection<EconomyDTOs.TraderTO> retSet = new ArrayList<>(entityOidToDto.size());
        try {
            logger.info("Converting topologyEntityDTOs to traderTOs");
            logger.debug("Start creating bicliques");
            BiMap<Long, String> oidToUuidMap = HashBiMap.create();
            for (TopologyDTO.TopologyEntityDTO dto : entityOidToDto.values()) {
                dto.getCommoditySoldListList().stream()
                    .filter(comm -> CommodityConverter.isBicliqueCommodity(comm.getCommodityType()))
                    .forEach(comm -> edge(dto, comm));
                oidToUuidMap.put(dto.getOid(), String.valueOf(dto.getOid()));
                populateCommodityConsumesTable(dto);
            }
            if (enableOP) {
                fakeEntityCreator.getHostIdToClusterId().entrySet().forEach(
                        entry -> pmBasedBicliquer.edge(String.valueOf(entry.getKey()), String.valueOf(entry.getValue())));
            }
            // Create market tier traderTO builders
            List<TraderTO.Builder> marketTierTraderTOBuilders = cloudTc.createMarketTierTraderTOs();
            marketTierTraderTOBuilders.forEach(t -> oidToUuidMap.put(t.getOid(), String.valueOf(t.getOid())));
            dsBasedBicliquer.compute(oidToUuidMap);
            pmBasedBicliquer.compute(oidToUuidMap);
            logger.debug("Done creating bicliques");

            // Convert market tier traderTO builders to traderTOs
            marketTierTraderTOBuilders.stream()
                    .map(t -> t.addAllCliques(pmBasedBicliquer.getBcIDs(String.valueOf(t.getOid()))))
                    .forEach(t -> {
                        final MarketTier marketTier = cloudTc.getMarketTier(t.getOid());
                        if (marketTier != null
                            && marketTier.getTier().getEntityType()
                            != EntityType.STORAGE_TIER_VALUE) {
                            t.addAllCommoditiesSold(
                                commodityConverter.bcCommoditiesSold(t.getOid()));
                        }
                        retSet.add(t.build());
                    });
            // Iterate over all scaling groups and compute top usage
            calculateScalingGroupUsageData(entityOidToDto);
            entityOidToDto.values().stream()
                    .filter(t -> TopologyConversionUtils.shouldConvertToTrader(t.getEntityType()))
                    .map(this::topologyDTOtoTraderTO)
                    .filter(Objects::nonNull)
                    .forEach(retSet::add);

            commodityConverter.clearProviderUsedSubtractionMap();

            logger.info("Converted topologyEntityDTOs to traderTOs");
            return retSet;
        } catch (Exception e) {
            logger.error(EconomyConstants.EXCEPTION_MESSAGE,
                "convertToMarket", e.getMessage(), e);
            return retSet;
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
     * @param projectedTraders list of {@link TraderTO}s that are to be converted to
     * {@link TopologyEntityDTO}s
     * @param originalTopology the original set of {@link TopologyEntityDTO}s by OID.
     * @param priceIndexMessage the price index message
     * @param reservedCapacityResults the reserved capacity results
     * @param wastedFileAnalysis wasted file analysis results
     * @return list of {@link TopologyDTO.ProjectedTopologyEntity}s
     */
    @Nonnull
    public Map<Long, TopologyDTO.ProjectedTopologyEntity> convertFromMarket(
                @Nonnull final List<EconomyDTOs.TraderTO> projectedTraders,
                @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology,
                @Nonnull final PriceIndexMessage priceIndexMessage,
                @Nonnull final ReservedCapacityResults reservedCapacityResults,
                @Nonnull final WastedFilesResults wastedFileAnalysis) {

        conversionErrorCounts.startPhase(Phase.CONVERT_FROM_MARKET);
        try {
            final Map<Long, PriceIndexMessagePayload> priceIndexByOid =
                priceIndexMessage.getPayloadList().stream()
                    .collect(Collectors.toMap(PriceIndexMessagePayload::getOid, Function.identity()));
            Map<Long, EconomyDTOs.TraderTO> projTraders =
                    projectedTraders.stream().collect(Collectors.toMap(TraderTO::getOid, Function.identity()));
            logger.info("Converting {} projectedTraders to topologyEntityDTOs", projectedTraders.size());
            projectedTraders.forEach(t -> oidToProjectedTraderTOMap.put(t.getOid(), t));
            projectedRICoverageCalculator.relinquishCoupons(projectedTraders);
            if (useVMReservationAsUsed) {
                generateProjectedProviderUsedSubtractionMap(projectedTraders, originalTopology);
            }
            final Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedTopologyEntities = new HashMap<>(
                projectedTraders.size());
            for (TraderTO projectedTrader : projectedTraders) {
                try {
                    final Set<TopologyEntityDTO> projectedEntities = traderTOtoTopologyDTO(
                            projectedTrader, originalTopology, reservedCapacityResults,
                            projTraders, wastedFileAnalysis);
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
                    projectedRICoverageCalculator.calculateProjectedRiCoverage(projectedTrader);
                } catch (Exception e) {
                    logger.error(EconomyConstants.EXCEPTION_MESSAGE,
                    projectedTrader.getDebugInfoNeverUseInCode(), e.getMessage(), e);
                }
            }
            return projectedTopologyEntities;
        } catch (Exception e) {
            logger.error(EconomyConstants.EXCEPTION_MESSAGE,
                "convertFromMarket", e.getMessage(), e);
            return new HashMap<>();
        } finally {
            conversionErrorCounts.endPhase();
        }
    }

    /**
     * Generate ProjectedProviderUsedSubtractionMap to update projected provider sold value
     * with buyer reserved value after market analysis
     * Map<providerOid, Map<the commodity type to update, the value to be subtracted from sold value>>
     *
     * @param projectedTraders list of {@link TraderTO}s that are to be converted to {@link TopologyEntityDTO}s.
     * @param originalTopology the original set of {@link TopologyEntityDTO}s by OID.
     */
    private void generateProjectedProviderUsedSubtractionMap(@Nonnull final List<TraderTO> projectedTraders,
                                                             @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology) {
        for (TraderTO trader : projectedTraders) {
            final long traderOid = trader.getOid();
            final TopologyDTO.TopologyEntityDTO originalEntity = originalTopology.get(traderOid);
            // Check if trader is part of commoditiesWithReservationGreaterThanUsed.
            // If so, it means the reserved capacity of a commodity is greater than its used value.
            // We need to update the sold value of its current supplier.
            if (originalEntity != null
                && commoditiesWithReservationGreaterThanUsed.containsKey(traderOid)
                && ENTITIES_AND_COMMODITIES_NEEDS_TO_CAP_RESERVATION.containsKey(originalEntity.getEntityType())) {
                for (ShoppingListTO sl : trader.getShoppingListsList()) {
                    final long supplierOid = sl.getSupplier();
                    final TopologyDTO.TopologyEntityDTO supplier = entityOidToDto.get(supplierOid);
                    if (supplier != null && ENTITIES_AND_COMMODITIES_NEEDS_TO_CAP_RESERVATION.get(originalEntity.getEntityType()).containsKey(supplier.getEntityType())) {
                        commoditiesWithReservationGreaterThanUsed.get(traderOid).keySet().stream()
                            .filter(commBought -> ENTITIES_AND_COMMODITIES_NEEDS_TO_CAP_RESERVATION.get(originalEntity.getEntityType())
                                .get(supplier.getEntityType()).contains(commBought.getCommodityType().getType()))
                            .forEach(commBought -> {
                                final Map<CommodityType, Double> subtraction =
                                    projectedProviderUsedSubtractionMap.computeIfAbsent(supplierOid, key -> new HashMap<>());
                                // Update the sold value of its current supplier by
                                // (commBought.getReservedCapacity() - commBought.getUsed())
                                subtraction.put(commBought.getCommodityType(),
                                    subtraction.getOrDefault(commBought.getCommodityType(), 0d)
                                        + (commBought.getReservedCapacity() - commBought.getUsed()));
                            });
                    }
                }
            }
        }
    }

    /**
     * Convert the {@link EconomyDTOs.TraderTO}s to {@link TopologyDTO.ProjectedTopologyEntity}s for
     * Buy RI Impact Analysis.
     *
     * <p>Here just create projected entities by copying the original entities.
     *
     * {@link TopologyDTO.TopologyEntityDTO}s
     * @param originalTopology the original set of {@link TopologyDTO.TopologyEntityDTO}s by OID.
     * @return list of {@link TopologyDTO.ProjectedTopologyEntity}s
     */
    @Nonnull
    public Map<Long, TopologyDTO.ProjectedTopologyEntity> createProjectedEntitiesAsCopyOfOriginalEntities(
                @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology) {
        final Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedTopologyEntities = new HashMap<>(
                        originalTopology.size());
        try {
            for (TopologyEntityDTO projectedEntity : originalTopology.values()) {
                final ProjectedTopologyEntity.Builder projectedEntityBuilder =
                    ProjectedTopologyEntity.newBuilder().setEntity(projectedEntity);
                projectedTopologyEntities.put(projectedEntity.getOid(), projectedEntityBuilder.build());
            }
        } catch (Exception e) {
            logger.error("Exception in createProjectedEntitiesAsCopyOfOriginalEntities", e);
        }
        return projectedTopologyEntities;
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
     * @return The {@link Action} describing the recommendation in a topology-specific way.
     */
    @Nonnull
    public List<Action> interpretAllActions(@Nonnull final List<ActionTO> actionTOs,
                                              @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                              @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                            @Nonnull CloudActionSavingsCalculator actionSavingsCalculator) {
        // Before beginning to interpret actions, compute the reason settings for the tier exclusion
        // actions
        tierExcluder.computeReasonSettings(actionTOs, originalCloudTopology);
        List<Action> actions = Lists.newArrayList();
        actionTOs.forEach(actionTO -> {
            List<Action> currentActions = interpretAction(actionTO, projectedTopology,
                originalCloudTopology, actionSavingsCalculator);
            actions.addAll(currentActions);
        });
        return actions;
    }

    /**
     * After actions have been interpreted, we don't need some state.
     * So we clear that.
     */
    public void clearStateNeededForActionInterpretation() {
        tierExcluder.clearStateNeededForActionInterpretation();
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
     * @param actionSavingsCalculator  Calculates cloud savings for an action.
     *
     *
     * @return The {@link Action} describing the recommendation in a topology-specific way.
     */
    @NonNull
    List<Action> interpretAction(@Nonnull final ActionTO actionTO,
                                 @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                 @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                 @Nonnull CloudActionSavingsCalculator actionSavingsCalculator) {
        return actionInterpreter.get().interpretAction(actionTO, projectedTopology, originalCloudTopology,
                actionSavingsCalculator);
    }

    /**
     * Create CommoditiesBoughtFromProvider for TopologyEntityDTO.
     *
     * @param sl       ShoppingListTO of the projectedTraderTO
     * @param commList List of CommodityBoughtDTO
     * @param buyerOid id of the buyer for which the CommoditiesBoughtFromProvider is being created
     * @param projTraders Map of OID to projected TraderTO. Used to look up providers for traders provisioned
     *                    by the market (ie not in the original topology) where the provider may also be
     *                    provisioned by the market.
     *
     * @return CommoditiesBoughtFromProvider
     */
    private CommoditiesBoughtFromProvider createCommoditiesBoughtFromProvider(
        ShoppingListTO sl, List<CommodityBoughtDTO> commList,
        final long buyerOid,
        @Nonnull final  Map<Long, TraderTO> projTraders) {
        final CommoditiesBoughtFromProvider.Builder commoditiesBoughtFromProviderBuilder =
            CommoditiesBoughtFromProvider.newBuilder().addAllCommodityBought(commList);
        // already created a backup info, cannot be null
        ShoppingListInfo slInfo = shoppingListOidToInfos.get(sl.getOid());
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
        } else if (skippedEntities.containsKey(slInfo.getSellerId())) {
            // If a sl is unplaced due to skipped provider, which makes it movable false,
            // we should set the supplier of it to the original skipped provider.
            // Why do we need else if here, not just else?
            // Because for a sl moving from active provider to unplaced, the supplier should be null.
            // If we just use else here, then the supplier will be the original active provider,
            // which is not correct.
            supplier = slInfo.getSellerId();
        }

        Integer supplierEntityType = null;
        if (supplier != null) {
            // if collapsedBuyerId is different from buyerOid, it means that the collapsedBuyerId is
            // the original (pre-collapsing) provider of the shopping list. If collapsedBuyerId
            // is same as buyerOid, it means that the current provider of the shopping list is
            // the same as the original provider
            if (slInfo.getCollapsedBuyerId().isPresent()
                    && slInfo.getCollapsedBuyerId().get() != buyerOid) {
                supplier = slInfo.getCollapsedBuyerId().get();
            }
            // If the supplier is a market tier, then get the tier TopologyEntityDTO and
            // make that the supplier.
            if (cloudTc.isMarketTier(supplier)) {
                if (cloudTc.getMarketTier(supplier).hasRIDiscount()) {
                    // Projected trader should not be supplied by RI Tier. But it can happen if
                    // costs are not yet fully discovered or there is a highly restrictive
                    // tier-exclusion policy. Both these situations prevent
                    // AnalysisToProtobuf#replaceNewSupplier from replacing the RITier by an
                    // on-demand tier. If this happens, we should make no changes and take the
                    // supplier of the shopping list from the original entity.
                    Optional<TopologyEntityDTO> computeTier = cloudTopology.getComputeTier(slInfo.getBuyerId());
                    if (computeTier.isPresent()) {
                        supplier = computeTier.get().getOid();
                        supplierEntityType = computeTier.get().getEntityType();
                    } else {
                        logger.error("{} does not have compute tier supplier", slInfo.getBuyerId());
                        TopologyEntityDTO tier = cloudTc.getMarketTier(supplier).getTier();
                        supplier = tier.getOid();
                        supplierEntityType = tier.getEntityType();
                    }
                } else {
                    TopologyEntityDTO tier = cloudTc.getMarketTier(supplier).getTier();
                    supplier = tier.getOid();
                    supplierEntityType = tier.getEntityType();
                }
            }
            commoditiesBoughtFromProviderBuilder.setProviderId(supplier);
        }
        // For a sl of an unplaced VM before market, it doesn't have a provider, but has
        // providerEntityType. It should remain the same if it's unplaced after market.
        // For a sl moving from active provider/unplaced to provisioned provider,
        // we can get the providerEntityType from slInfo.
        if (supplierEntityType != null) {
            commoditiesBoughtFromProviderBuilder.setProviderEntityType(supplierEntityType);
        } else {
            final TopologyEntityDTO supplierEntity = entityOidToDto.get(supplier);
            if (supplierEntity != null) {
                commoditiesBoughtFromProviderBuilder
                        .setProviderEntityType(supplierEntity.getEntityType());
            } else {
                final Optional<Integer> sellerEntityType = slInfo.getSellerEntityType();
                if (sellerEntityType.isPresent()) {
                    commoditiesBoughtFromProviderBuilder.setProviderEntityType(sellerEntityType.get());
                } else if (supplier != null) {
                    TraderTO projSupplier = projTraders.get(supplier);
                    if (projSupplier != null) {
                        commoditiesBoughtFromProviderBuilder.setProviderEntityType(projSupplier.getType());
                    }
                }
            }
        }
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
     * @param reservedCapacityResults the reserved capacity results
     * @param projTraders projected traders
     * @param wastedFileResults wasted file analysis results.
     * @return set of {@link TopologyDTO.TopologyEntityDTO}s
     */
    @VisibleForTesting
    Set<TopologyDTO.TopologyEntityDTO> traderTOtoTopologyDTO(EconomyDTOs.TraderTO traderTO,
                    @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> traderOidToEntityDTO,
                    @Nonnull final ReservedCapacityResults reservedCapacityResults,
                    @Nonnull final Map<Long, EconomyDTOs.TraderTO> projTraders,
                    @Nonnull final WastedFilesResults wastedFileResults) {
        Set<TopologyDTO.TopologyEntityDTO> topologyEntityDTOs = Sets.newHashSet();
        try {
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
            final List<ShoppingListTO> collapsedShoppingLists = new ArrayList<>();

            // stores information required to create extra projected entities for a buyer->seller link. The value of
            // commBought2shoppingListWithResources is a map in order to allow creation of multiple volumes
            // (represented in ShoppingListInfo) mapped to the same CommoditiesBoughtFromProvider
            final Map<CommoditiesBoughtFromProvider, Map<ShoppingListTO, ShoppingListInfo>> commBought2shoppingListWithResources =
                            new HashMap<>();

            // If traderTO is a cloned VM, use the shoppingLists from it's original VM to create
            // CommoditiesBoughtFromProvider. Provisioned VMs do not have providerIDs populated in
            // corresponding shoppingLists, so this is to make sure provisioned VMs won't be incorrectly
            // recognized as unplaced.
            List<ShoppingListTO> shoppingLists = traderTO.getType() == EntityType.VIRTUAL_MACHINE_VALUE
                && projTraders.get(traderTO.getCloneOf()) != null
                ? projTraders.get(traderTO.getCloneOf()).getShoppingListsList()
                : traderTO.getShoppingListsList();
            for (EconomyDTOs.ShoppingListTO sl : shoppingLists) {
                List<TopologyDTO.CommodityBoughtDTO> commList = new ArrayList<>();
                // Map to keep timeslot commodities from the generated timeslot families
                // This is needed because we need to add those timeslots coming from the market later
                // to generated commodity DTOs
                final Map<CommodityType, List<Double>> timeSlotsByCommType = Maps.newHashMap();
                // First we merge timeslot commodities
                final Collection<CommodityDTOs.CommodityBoughtTO.Builder> commBuilders =
                    sl.getCommoditiesBoughtList().stream().map(CommodityBoughtTO::toBuilder)
                    .collect(Collectors.toList());
                final Collection<CommodityDTOs.CommodityBoughtTO> mergedCommoditiesBought =
                    mergeTimeSlotCommodities(commBuilders, timeSlotsByCommType,
                        CommodityBoughtTO.Builder::getSpecification,
                        CommodityBoughtTO.Builder::getQuantity,
                        CommodityBoughtTO.Builder::getPeakQuantity,
                        CommodityBoughtTO.Builder::setQuantity,
                        CommodityBoughtTO.Builder::setPeakQuantity)
                    .stream().map(CommodityBoughtTO.Builder::build).collect(Collectors.toList());
                for (CommodityDTOs.CommodityBoughtTO commBought : mergedCommoditiesBought) {
                    // if the commodity bought DTO type is biclique, create new
                    // DSPM and Datastore bought
                    if ( commodityConverter.isSpecBiClique(commBought.getSpecification().getBaseType())) {
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
                    } else if (tierExcluder.isCommSpecTypeForTierExclusion(commBought.getSpecification())) {
                        // If the commodity bought was created by market-component for tier exclusion,
                        // then do not include it in the projected topology.
                        continue;
                    } else {
                        //If the traderTO has cloneOf, it is a provisioned SE.
                        commBoughtTOtoCommBoughtDTO(traderTO.getOid(), sl.getSupplier(), sl.getOid(),
                            commBought, reservedCapacityResults, originalEntity, timeSlotsByCommType,
                                traderTO.hasCloneOf()).ifPresent(commList::add);
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
                                    sl.getSupplier(), Collections.emptySet(), null,
                                    supplier != null ? supplier.getType() : null, commList);
                    shoppingListOidToInfos.put(sl.getOid(), slInfo);
                }
                final ShoppingListInfo shoppingListInfo = shoppingListOidToInfos.get(sl.getOid());
                if (shoppingListInfo != null && shoppingListInfo.getCollapsedBuyerId().isPresent()) {
                    collapsedShoppingLists.add(sl);
                }
                CommoditiesBoughtFromProvider commoditiesBought =
                        createCommoditiesBoughtFromProvider(sl, commList, traderTO.getOid(), projTraders);
                topoDTOCommonBoughtGrouping.add(commoditiesBought);
                if (!CollectionUtils.isEmpty(shoppingListInfo.getResourceIds())) {
                    // there can be multiple volume shoppingLists with the same usages and providers. Cache multiple
                    // potential sl:slInfo mappings for the same commBoughtGrouping
                    commBought2shoppingListWithResources
                            .computeIfAbsent(commoditiesBought, slMapping -> new HashMap<>()).put(sl, shoppingListInfo);
                }
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
                    final TraderStateTO originalStateTo = TopologyConversionUtils.traderState(originalEntity);
                    if (isSameVMTraderState(traderTO.getState(), originalStateTo)) {
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
                    .setControllable(traderSetting.getControllable())
                    .setSuspendable(traderSetting.getSuspendable())
                    .setDesiredUtilizationTarget((traderSetting.getMaxDesiredUtilization()
                           + traderSetting.getMinDesiredUtilization()) / 2)
                    .setDesiredUtilizationRange(traderSetting.getMaxDesiredUtilization()
                           - traderSetting.getMinDesiredUtilization())
                    .setProviderMustClone(traderSetting.getProviderMustClone())
                    .setDaemon(traderSetting.getDaemon())
                .build();
            TopologyDTO.TopologyEntityDTO.Builder entityDTOBuilder =
                    TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setEntityType(traderTO.getType())
                        .setOid(traderTO.getOid())
                        .setEntityState(entityState)
                        .setDisplayName(displayName)
                        .addAllCommoditySoldList(retrieveCommSoldList(traderTO, traderOidToEntityDTO))
                        .addAllCommoditiesBoughtFromProviders(topoDTOCommonBoughtGrouping)
                        .addAllConnectedEntityList(getConnectedEntities(traderTO))
                        .setAnalysisSettings(analysisSetting);
            if (originalEntity == null) {
                // this is a clone trader
                originalEntity = traderOidToEntityDTO.get(traderTO.getCloneOf());
                long originalEntityOid = originalEntity.getOid();
                /*
                 * If the original entity is a plan entity, we need to use that original entity's
                 * original (realtime) entity instead. We need to do this for cases where the plan
                 * topology edit clones an entity but does not reuse the original entity's OID (for
                 * example, HCI template expansion does this). In these cases, these new entities
                 * are not accessible via the API.  To be compatible with existing plan output, we
                 * want the clones to link back to the original realtime entity.
                 */
                if (originalEntity.hasOrigin()) {
                    Origin originalEntityOrigin = originalEntity.getOrigin();
                    if (originalEntityOrigin.hasPlanScenarioOrigin()) {
                        originalEntityOid = originalEntityOrigin.getPlanScenarioOrigin()
                                .getOriginalEntityId();
                    }
                }
                entityDTOBuilder.setOrigin(Origin.newBuilder()
                    .setAnalysisOrigin(AnalysisOrigin.newBuilder()
                        .setOriginalEntityId(originalEntityOid)));
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
            wastedFileResults.getMbReleasedOnProvider(traderTO.getOid())
                .ifPresent(stAmtToReleaseInMB -> {
                    entityDTOBuilder.getCommoditySoldListBuilderList().stream()
                        .filter(commSold -> commSold.getCommodityType().getType() == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                        .findFirst()
                        .ifPresent(storageCommSold ->
                            storageCommSold.setUsed(Math.max(0, storageCommSold.getUsed() - stAmtToReleaseInMB)));
                });

            // Only call this if the entity is not produced by analysis
            if (!entityDTOBuilder.getOrigin().hasAnalysisOrigin()) {
                overwriteCommoditiesBoughtByVMsFromVolumes(entityDTOBuilder);
            }

            updateProjectedEntityOsType(entityDTOBuilder);
            updateProjectedCores(entityDTOBuilder);

            // TODO (on-prem vvs p2) the subsequent two control flows should be merged back into common logic
            // both cloud and on-prem will situationally want to use collapsed buyers technique
            // there is no more single volume id related to a SL in either environment type
            // when source volume is on-prem volume with volumeId
            topologyEntityDTOs.addAll(createResources(entityDTOBuilder, commBought2shoppingListWithResources));
            // when source volume is cloud volume with collapsedBuyerId
            topologyEntityDTOs.addAll(createCollapsedTopologyEntityDTOs(entityDTOBuilder, collapsedShoppingLists,
                            reservedCapacityResults, projTraders));
            TopologyEntityDTO entityDTO = entityDTOBuilder.build();
            topologyEntityDTOs.add(entityDTO);
        } catch (Exception e) {
            logger.error(EconomyConstants.EXCEPTION_MESSAGE,
                traderTO.getDebugInfoNeverUseInCode(), e.getMessage(), e);
        }
        return topologyEntityDTOs;
    }

    /**
     * Create {@link TopologyEntityDTO}s for collapsed shopping lists.
     *
     * @param projectedEntityForConsumerOfCollapsedEntity projected entity for consumer of collapsed entity
     * @param collapsedShoppingList shopping list with collapsedBuyerId
     * @param reservedCapacityResults reserved capacity
     * @param projTraders projected traders
     * @return a list of {@link TopologyEntityDTO}s
     */
    private List<TopologyEntityDTO> createCollapsedTopologyEntityDTOs(
        final TopologyEntityDTOOrBuilder projectedEntityForConsumerOfCollapsedEntity,
        final List<ShoppingListTO> collapsedShoppingList,
        final ReservedCapacityResults reservedCapacityResults,
        final Map<Long, TraderTO> projTraders) {
        final List<TopologyEntityDTO> result = new ArrayList<>();
        for (final ShoppingListTO shoppingListTO : collapsedShoppingList) {
            final Optional<Long> collapsedEntityId = Optional.ofNullable(shoppingListOidToInfos
                .get(shoppingListTO.getOid()))
                .flatMap(ShoppingListInfo::getCollapsedBuyerId);
            if (collapsedEntityId.isPresent()) {
                final TopologyEntityDTO collapsedEntityDTO = entityOidToDto
                    .get(collapsedEntityId.get());
                final TopologyEntityDTO.Builder projectedEntity = TopologyEntityDTO.newBuilder()
                    .setOid(collapsedEntityDTO.getOid())
                    .setEntityType(collapsedEntityDTO.getEntityType())
                    .setDisplayName(collapsedEntityDTO.getDisplayName());

                final List<CommodityBoughtDTO> boughtCommodityDTOS = shoppingListTO
                    .getCommoditiesBoughtList().stream()
                    .map(commodityBoughtTO -> {
                        // If the CommodityBoughtTO type is biclique, skip it in projected topology.
                        if (commodityConverter.isSpecBiClique(commodityBoughtTO.getSpecification().getBaseType())) {
                            return Optional.<CommodityBoughtDTO>empty();
                        } else {
                            return commBoughtTOtoCommBoughtDTO(
                                collapsedEntityDTO.getOid(), shoppingListTO.getSupplier(),
                                shoppingListTO.getOid(), commodityBoughtTO, reservedCapacityResults,
                                collapsedEntityDTO, new HashMap<>(), false);
                        }
                    })
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
                CommoditiesBoughtFromProvider commBoughtGrouping = createCommoditiesBoughtFromProvider(
                        shoppingListTO, boughtCommodityDTOS, collapsedEntityDTO.getOid(), projTraders);
                projectedEntity.addCommoditiesBoughtFromProviders(commBoughtGrouping);
                final List<CommoditySoldDTO> soldDTOS =
                    createCommoditySoldFromCommBoughtTO(collapsedEntityDTO,
                        shoppingListTO.getCommoditiesBoughtList());
                projectedEntity.addAllCommoditySoldList(soldDTOS);
                copyStaticAttributes(collapsedEntityDTO, projectedEntity);
                // copy the origin from the original entity.
                if (collapsedEntityDTO.hasOrigin()) {
                    projectedEntity.setOrigin(collapsedEntityDTO.getOrigin());
                }
                // Connect collapsed entity with the AZ/Region of its consumer's AZ/Region.
                // i.e. For cloud volumes, connect projected cloud volumes with the same AZ/Region
                // as the projected VM that the volumes are attached to.
                createConnectedAzOrRegion(projectedEntityForConsumerOfCollapsedEntity)
                        .ifPresent(projectedEntity::addConnectedEntityList);
                result.add(projectedEntity.build());
            }
        }
        logger.trace("Collapsed Projected Traders created: {}", () -> result);
        return result;
    }

    /**
     * Populate the commodities sold of collpased entity based on its commodity bought.
     *
     * @param entity A collpased entity.
     * @param commodityBoughtTOS the list of commodity bought DTOs
     * @return a list of CommoditySoldDTOs.
     */
    private List<CommoditySoldDTO> createCommoditySoldFromCommBoughtTO(
        final TopologyEntityDTO entity, final List<CommodityBoughtTO> commodityBoughtTOS) {
        final List<CommoditySoldDTO> result = new ArrayList<>();
        for (final CommodityBoughtTO commodityBoughtTO : commodityBoughtTOS) {
            final CommodityType commodityType =
                commodityConverter.commodityIdToCommodityType(commodityBoughtTO.getSpecification()
                    .getType());
            double newCapacity = commodityBoughtTO.getAssignedCapacityForBuyer();
            if (newCapacity <= 0) {
                newCapacity = getCommodityIndex().getCommSold(entity.getOid(), commodityType)
                    .map(CommoditySoldDTO::getCapacity).orElse(0D);
            }
            newCapacity =
                TopologyConversionUtils.convertMarketUnitToTopologyUnit(commodityType.getType(),
                    newCapacity, entity, isCloudMigration);
            final CommoditySoldDTO.Builder soldBuilder = CommoditySoldDTO.newBuilder()
                .setCommodityType(commodityType)
                .setCapacity(newCapacity);
            final CommoditySoldDTO currentCommSold =
                getCommodityIndex().getCommSold(entity.getOid(), commodityType)
                .orElse(null);
            if (currentCommSold != null) {
                soldBuilder.setUsed(currentCommSold.getUsed());
                if (newCapacity > 0 && currentCommSold.hasHistoricalUsed()
                    && currentCommSold.getHistoricalUsed().hasPercentile()) {
                    final double oldCapacity = currentCommSold.getCapacity();
                    final double oldPercentile =
                        currentCommSold.getHistoricalUsed().getPercentile();
                    final double projectedPercentile = oldPercentile * oldCapacity / newCapacity;
                    soldBuilder
                        .setHistoricalUsed(HistoricalValues.newBuilder()
                            .setPercentile(projectedPercentile)
                            .build());
                }
            }
            result.add(soldBuilder.build());
        }
        return result;
    }

    /**
     * This method overwrites projected commodities bought by VMs from volumes using original
     * commodities. That is each projected commodity bought by VM from volume is replaced with
     * related commodity from original VM {@code TopologyEntityDTO}.
     *
     * @param entityBuilder Projected entity builder.
     */
    private void overwriteCommoditiesBoughtByVMsFromVolumes(
            @Nonnull final TopologyEntityDTO.Builder entityBuilder) {
        if (entityBuilder.getEntityType() != EntityType.VIRTUAL_MACHINE.getNumber()) {
            return;
        }

        final TopologyEntityDTO originalVm = entityOidToDto.get(entityBuilder.getOid());
        if (originalVm == null) {
            logger.error("Cannot find original entity for projected VM: " + entityBuilder.getOid());
            return;
        }

        // Get original commodities
        final Queue<CommoditiesBoughtFromProvider> originalCommodities =
                originalVm.getCommoditiesBoughtFromProvidersList()
                        .stream()
                        .filter(commBought -> isValidVolume(entityBuilder, commBought))
                        .collect(Collectors.toCollection(LinkedList::new));

        if (originalCommodities.isEmpty()) {
            return;
        }

        // Replace every commodity bought from volume with original commodity. We assume that the
        // number of commodities hasn't changed.
        for (int i = 0; i < entityBuilder.getCommoditiesBoughtFromProvidersCount(); i++) {
            final long providerEntityType = entityBuilder
                    .getCommoditiesBoughtFromProvidersBuilder(i).getProviderEntityType();
            if (providerEntityType == EntityType.VIRTUAL_VOLUME.getNumber()) {
                final CommoditiesBoughtFromProvider nextCommBought = originalCommodities.poll();
                if (nextCommBought == null) {
                    logger.error("The number of projected commodities bought by VM {} from "
                                    + "Volumes is greater than the number of original commodities",
                            entityBuilder.getOid());
                    return;
                }
                entityBuilder.setCommoditiesBoughtFromProviders(i, nextCommBought.toBuilder());
            }
        }

        if (!originalCommodities.isEmpty()) {
            logger.error("The number of projected commodities bought by VM {} from Volumes "
                    + "doesn't match the number of original commodities. {} extra original "
                    + "commodities found", entityBuilder.getOid(), originalCommodities.size());
        }
    }

    /**
     * When converting a topology entity to a traderTO, we do not include shopping lists for
     * ephemeral volumes, which puts the original and projected comm bought lists out of sync.
     * Since overwriteCommoditiesBoughtByVMsFromVolumes() expects the comm bought lists to be the
     * same, we must also omit the ephemeral volumes here.
     *
     * @param entityBuilder builder for related entity, used for logging
     * @param commBought commodity to check
     * @return true if the volume commodity should be present in the projected entity.
     */
    private boolean isValidVolume(TopologyEntityDTO.Builder entityBuilder,
            CommoditiesBoughtFromProvider commBought) {
        if (commBought.getProviderEntityType() != EntityType.VIRTUAL_VOLUME.getNumber()) {
            return false;
        }
        final TopologyEntityDTO originalVolume = entityOidToDto.get(commBought.getProviderId());
        if (originalVolume == null) {
            // Can't check whether the SL was skipped, so let it pass
            logger.warn("Cannot determine whether provider {} for entity {} is an ephemeral "
                            + "volume - assuming it is not",
                    commBought.getProviderId(), entityBuilder.getDisplayName());
            return true;
        }
        // If the shopping list for this volume was skipped when passing to the market, it
        // won't appear in the projected topology and is therefore not valid.
        return !skipShoppingListCreation(originalVolume, commBought);
    }

    /**
     * Reads the license access commodity key value (e.g 'SUSE') and updates it into the VM type
     * specific info in the projected entity, if it happens to be different. Applicable for cloud
     * migration case where we possibly migrate from one OS type to another. In such cases,
     * projected entity's VM type specific info points to the new OS type.
     *
     * @param builder Projected entity builder to update.
     */
    @VisibleForTesting
    void updateProjectedEntityOsType(@Nonnull final TopologyEntityDTO.Builder builder) {
        if (builder.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            return;
        }
        final TypeSpecificInfo typeInfo = builder.getTypeSpecificInfo();
        if (!typeInfo.hasVirtualMachine()) {
            return;
        }
        final Optional<String> optionalNewOsType = Optional.ofNullable(builder
                .getEntityPropertyMapMap().get(StringConstants.PLAN_NEW_OS_TYPE_PROPERTY));
        if (!optionalNewOsType.isPresent()) {
            return;
        }
        builder.removeEntityPropertyMap(StringConstants.PLAN_NEW_OS_TYPE_PROPERTY);
        final CloudCostDTO.OSType newOsType = Enums.getIfPresent(OSType.class,
                optionalNewOsType.get()).orNull();
        final CloudCostDTO.OSType oldOsType = typeInfo.getVirtualMachine().getGuestOsInfo()
                .getGuestOsType();
        String newOsName = builder.getEntityPropertyMapMap().get(
                StringConstants.PLAN_NEW_OS_NAME_PROPERTY);
        builder.removeEntityPropertyMap(StringConstants.PLAN_NEW_OS_NAME_PROPERTY);
        if (newOsType == null || newOsName == null) {
            return;
        }
        // We want to set the OS name in projected always to be OsType.displayName
        // (e.g 'Linux (Free)') as that is what we show in user visible mapping tables.
        logger.trace("Updating OS type for projected entity {} from {} -> {}.",
                builder.getDisplayName(), oldOsType, newOsType);
        final TypeSpecificInfo.Builder typeInfoBuilder = TypeSpecificInfo.newBuilder(typeInfo)
                .setVirtualMachine(VirtualMachineInfo
                .newBuilder(typeInfo.getVirtualMachine())
                .setGuestOsInfo(TopologyDTO.OS.newBuilder()
                        .setGuestOsType(newOsType)
                        .setGuestOsName(newOsName).build()));
        builder.setTypeSpecificInfo(typeInfoBuilder);
    }

    /**
     * Update the number of CPU cores in a projected VM
     * based on the number of cores that its new Compute Tier has.
     * This is important so that calculations such as projected
     * OS license costs, which can vary with the number of cores, are
     * correct. This method is safe to call on any entity.
     * If the entity is not a VM that will be buying from a Compute Tier,
     * the entity will not be updated.
     *
     * @param entityBuilder Projected entity builder.
     */
    @VisibleForTesting
    void updateProjectedCores(@Nonnull final TopologyEntityDTO.Builder entityBuilder) {
        if (entityBuilder.getEntityType() != EntityType.VIRTUAL_MACHINE.getNumber()) {
            return;
        }

        entityBuilder.getCommoditiesBoughtFromProvidersList().stream()
            .filter(commBought -> commBought.getProviderEntityType()
                == EntityType.COMPUTE_TIER.getNumber())
            .map(CommoditiesBoughtFromProvider::getProviderId)
            .forEach(tierId -> {
                final TopologyEntityDTO newComputeTier = entityOidToDto.get(tierId);
                if (newComputeTier == null) {
                    logger.error("Cannot find compute tier for projected VM: " + entityBuilder.getOid());
                    return;
                }

                int newCores = (int)newComputeTier.getTypeSpecificInfo().getComputeTier().getNumOfCores();

                entityBuilder.setTypeSpecificInfo(
                    TypeSpecificInfo.newBuilder().setVirtualMachine(
                        entityBuilder.getTypeSpecificInfo().getVirtualMachine().toBuilder()
                            .setNumCpus(newCores)
                    )
                );
            });
    }

    /**
     * Merge timeslot commodities from the original commodities list.
     *
     * <p>Timeslot commodities from the same family generated for a specific {@link CommodityType} are merged
     * by averaging their use and peak values.
     * Non-timeslot commodities are left unchanged.
     * @param commodityBuilderTOs Original commodity builders bought or sold by trader
     * @param timeSlotsByCommType Holder for timeslot values arranged by commodity type
     * @param commoditySpecExtractor Function to extract commodity TO specification
     * @param quantityExtractor Function to extract commodity TO quantity
     * @param peakQuantityExtractor Function to extract commodity TO peak quantity
     * @param quantitySetter Function to set commodity TO quantity
     * @param peakQuantitySetter Function to set commodity TO peak quantity
     * @param <T> {@link CommodityBoughtTO.Builder} or {@link CommoditySoldTO.Builder}.
     * @return Merged commodity builders
     */
    @Nonnull
    @VisibleForTesting
    <T> Collection<T> mergeTimeSlotCommodities(
        @Nonnull final Collection<T> commodityBuilderTOs,
        @Nonnull final Map<CommodityType, List<Double>> timeSlotsByCommType,
        @Nonnull final Function<T, CommoditySpecificationTO> commoditySpecExtractor,
        @Nonnull final Function<T, Float> quantityExtractor,
        @Nonnull final Function<T, Float> peakQuantityExtractor,
        @Nonnull final BiFunction<T, Float, T> quantitySetter,
        @Nonnull final BiFunction<T, Float, T> peakQuantitySetter) {
        final List<T> mergedComms = Lists.newArrayListWithCapacity(
            commodityBuilderTOs.size());
        //final boolean isBought = commodityTOs.iterator().next() instanceof CommodityBoughtTO;
        final Map<CommodityType, List<T>> timeSlotCommoditiesByCommType = Maps.newHashMap();
        // group time slot commodities by CommodityType
        commodityBuilderTOs.forEach(commTO -> {
            final CommoditySpecificationTO commSpec = commoditySpecExtractor.apply(commTO);
            if (commodityConverter.isTimeSlotCommodity(commSpec)) {
                final Optional<TopologyDTO.CommodityType> commTypeOptional =
                    commodityConverter.marketToTopologyCommodity(commSpec);
                if (commTypeOptional.isPresent()) {
                    final CommodityType commType = commTypeOptional.get();
                    timeSlotCommoditiesByCommType.computeIfAbsent(commType,
                        k -> Lists.newLinkedList()).add(commTO);
                } else {
                    logger.error("Unknown commodity type returned by analysis for market id {}",
                        () -> commSpec.getType());
                }
            } else {
                mergedComms.add(commTO);
            }
        });
        mergeTimeSlotCommoditiesByType(timeSlotCommoditiesByCommType, timeSlotsByCommType, mergedComms,
            quantityExtractor, peakQuantityExtractor, quantitySetter, peakQuantitySetter);

        return mergedComms;
    }

    /**
     * Merge timeslot commodities arranged by {@link CommodityType}.
     *
     * @param timeSlotCmmoditiesByCommType Commodity TOs arranged by {@link CommodityType}
     * @param timeSlotsByCommType Holder for timeslot values arranged by commodity type
     * @param mergedCommodities Holder for merged commodities
     * @param quantityExtractor Function to extract commodity TO quantity
     * @param peakQuantityExtractor Function to extract commodity TO peak quantity
     * @param quantitySetter Function to set commodity TO quantity
     * @param peakQuantitySetter Function to set commodity TO peak quantity
     * @param <T> {@link CommodityBoughtTO} or {@link CommoditySoldTO}
     */
    private static <T> void mergeTimeSlotCommoditiesByType(
        @Nonnull final Map<CommodityType, List<T>> timeSlotCmmoditiesByCommType,
        @Nonnull final Map<CommodityType, List<Double>> timeSlotsByCommType,
        @Nonnull final Collection<T> mergedCommodities,
        @Nonnull final Function<T, Float> quantityExtractor,
        @Nonnull final Function<T, Float> peakQuantityExtractor,
        @Nonnull final BiFunction<T, Float, T> quantitySetter,
        @Nonnull final BiFunction<T, Float, T> peakQuantitySetter) {
       /* Sample timeSlotComms content with 2 time slots for pool CPU commodity (base type = 89):
               [type: 89 => [
                   specification {
                      type: 100
                      base_type: 89
                    }
                    quantity: 0.5
                    peak_quantity: 0.55,
                    specification {
                      type: 101
                      base_type: 89
                    }
                    quantity: 0.6
                    peak_quantity: 0.65
                ]
        */
        timeSlotCmmoditiesByCommType.forEach((commType, groupedCommodities) -> {
            final List<Double> timeSlotValues = Lists.newLinkedList();
            // ensure the generated family is sorted in the right order of slots
            groupedCommodities.sort(Comparator.comparing(comm -> commType.getType()));
            // we merge commodities from each generated family by averaging up used/peak values
            float totalQuantity = 0.0f;
            float totalPeak = 0.0f;
            for (final T comm : groupedCommodities) {
                final float currQuantity = quantityExtractor.apply(comm);
                timeSlotValues.add(Double.valueOf(currQuantity));
                totalQuantity += currQuantity;
                totalPeak += peakQuantityExtractor.apply(comm);
            }
            final T mergedCommBuilder = groupedCommodities.iterator().next();
            quantitySetter.apply(mergedCommBuilder, totalQuantity / groupedCommodities.size());
            peakQuantitySetter.apply(mergedCommBuilder, totalPeak / groupedCommodities.size());
            mergedCommodities.add(mergedCommBuilder);
            timeSlotsByCommType.put(commType, timeSlotValues);
        });
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
            // Special case for Cloud-to-Cloud migration: Spot (Bidding) instances should be
            // converted to On Demand instances.
            handleBiddingVm(source, destination);
        }

        // Copy over entity property map.
        Map<String, String> propertyMap = source.getEntityPropertyMapMap();
        if (propertyMap != null) {
            destination.putAllEntityPropertyMap(propertyMap);
        }
    }

    private void handleBiddingVm(TopologyEntityDTO source, TopologyEntityDTO.Builder destination) {
        final TypeSpecificInfo typeSpecificInfo = source.getTypeSpecificInfo();
        if (isCloudMigration && typeSpecificInfo.hasVirtualMachine()
                && typeSpecificInfo.getVirtualMachine().getBillingType() == VMBillingType.BIDDING) {
            destination.getTypeSpecificInfoBuilder().getVirtualMachineBuilder()
                    .setBillingType(VMBillingType.ONDEMAND);
        }
    }

    /**
     * Create entities for resources of topologyEntityDTO.
     * For ex. If a Cloud VM has a volume, then we create the projected version of the volume here.
     *
     * @param topologyEntityDTO The entity for which the resources need to be created
     * @param commBought2shoppingListWithResources maps commbought to related shopping list
     * @return set of resources
     */
    @VisibleForTesting
    Set<TopologyEntityDTO> createResources(TopologyEntityDTO.Builder topologyEntityDTO,
                    Map<CommoditiesBoughtFromProvider, Map<ShoppingListTO, ShoppingListInfo>> commBought2shoppingListWithResources) {
        Set<TopologyEntityDTO> resources = Sets.newHashSet();
        final Collection<CommoditiesBoughtFromProvider> cbfpWithUpdatedProvider = new HashSet<>();
        for (int i = topologyEntityDTO.getCommoditiesBoughtFromProvidersCount() - 1; i >= 0; i--) {
            final CommoditiesBoughtFromProvider commBoughtGrouping = topologyEntityDTO.getCommoditiesBoughtFromProviders(i);
            // create entities for volumes
            Map<ShoppingListTO, ShoppingListInfo> shoppingListMapping = commBought2shoppingListWithResources.get(commBoughtGrouping);
            Optional<Map.Entry<ShoppingListTO, ShoppingListInfo>> shoppingListEntry = shoppingListMapping == null ? null
                    : shoppingListMapping.entrySet().stream().findAny();
            if (shoppingListMapping == null || !shoppingListEntry.isPresent() || CollectionUtils
                            .isEmpty(shoppingListEntry.get().getValue().getResourceIds())) {
                // no extra resources to create for this market link
                continue;
            }
            final ShoppingListTO shoppingList = shoppingListEntry.get().getKey();
            final ShoppingListInfo slInfo = shoppingListEntry.get().getValue();
            shoppingListMapping.remove(shoppingListEntry.get().getKey());
            for (Long sourceVolumeId : slInfo.getResourceIds()) {
                TopologyEntityDTO originalVolume = entityOidToDto.get(sourceVolumeId);
                if (originalVolume != null && originalVolume.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    if (!commBoughtGrouping.hasProviderId() || !commBoughtGrouping.hasProviderEntityType()) {
                        logger.error("commBoughtGrouping of projected entity {} has volume Id {} " +
                            "but no associated storage or storageTier",
                            topologyEntityDTO.getDisplayName(), sourceVolumeId);
                        continue;
                    }
                    // Build a volume which is connected to the same Storage or StorageTier
                    // (which is the provider for this commBoughtGrouping), and connected to
                    // the same AZ as the VM if the zone exists
                    TopologyEntityDTO.Builder volume =
                        TopologyEntityDTO.newBuilder()
                            .setEntityType(originalVolume.getEntityType())
                            .setOid(originalVolume.getOid());

                    // connect to storage tier - regardless of commodities presence
                    // TODO on-prem vvs p2 - do not connect to storage when commodities are added
                    final ConnectedEntity connectedStorage = ConnectedEntity.newBuilder()
                            .setConnectedEntityId(commBoughtGrouping.getProviderId())
                            .setConnectedEntityType(commBoughtGrouping.getProviderEntityType())
                            .setConnectionType(ConnectionType.NORMAL_CONNECTION).build();
                    volume.addConnectedEntityList(connectedStorage);

                    // Get the AZ or Region the VM is connected to (there is no zone for azure or on-prem)
                    List<TopologyEntityDTO> azOrRegion = TopologyDTOUtil.getConnectedEntitiesOfType(
                        topologyEntityDTO,
                        Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE),
                        entityOidToDto);
                    if (!azOrRegion.isEmpty()) {
                        // Use the first AZ or Region we get.
                        ConnectedEntity connectedAzOrRegion = ConnectedEntity.newBuilder()
                            .setConnectedEntityId(azOrRegion.get(0).getOid())
                            .setConnectedEntityType(azOrRegion.get(0).getEntityType())
                            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                            .build();
                        volume.addConnectedEntityList(connectedAzOrRegion);
                    }

                    // TODO on-prem vvs p2 - change this condition for creating projected commodities
                    if (commBoughtGrouping.getProviderEntityType() == EntityType.STORAGE_TIER_VALUE) {
                        createVolumeCommodities(commBoughtGrouping, shoppingList, volume);

                        // Change VM to make it buy from new volume
                        final CommoditiesBoughtFromProvider.Builder changedCommodityBoughtGrouping =
                                        commBoughtGrouping.toBuilder();
                        // filter out commodities that have not been sold by VV
                        for (int j = 0; j < changedCommodityBoughtGrouping.getCommodityBoughtCount(); j++) {
                            final int commodityType = changedCommodityBoughtGrouping.getCommodityBought(j)
                                            .getCommodityType().getType();
                            if (!OLD_QUANTITY_REQUIRED_COMM_TYPES.contains(commodityType)) {
                                changedCommodityBoughtGrouping.removeCommodityBought(j);
                            }
                        }
                        changedCommodityBoughtGrouping.setProviderId(volume.getOid());
                        changedCommodityBoughtGrouping.setProviderEntityType(volume.getEntityType());
                        cbfpWithUpdatedProvider.add(changedCommodityBoughtGrouping.build());
                        topologyEntityDTO.removeCommoditiesBoughtFromProviders(i);
                    } else {
                        // connect vm to volume
                        final ConnectedEntity connectedVolume = ConnectedEntity.newBuilder()
                                        .setConnectedEntityId(volume.getOid())
                                        .setConnectedEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                                        .setConnectionType(ConnectionType.NORMAL_CONNECTION).build();
                        topologyEntityDTO.addConnectedEntityList(connectedVolume);
                    }

                    copyStaticAttributes(originalVolume, volume);
                    String optionalDisplayName = slInfo.getResourceDisplayName();
                    volume.setDisplayName(StringUtils.isEmpty(optionalDisplayName)
                                    ? originalVolume.getDisplayName()
                                    : optionalDisplayName);
                    resources.add(volume.build());
                }
            }
        }
        topologyEntityDTO.addAllCommoditiesBoughtFromProviders(cbfpWithUpdatedProvider);
        return resources;
    }

    /**
     * Create commodities of the projected entity that was collapsed as buyer.
     * Example: when volume is on cloud, or on-prem per-volume analysis is turned on.
     * Volumes don't have a corresponding trader object.
     * The commodity values are taken from the commodity bought list of the VM.
     *
     * @param commBoughtGrouping source commodities bought by vm
     * @param shoppingList corresponding shopping list
     * @param volume volume to add commodities to
     */
    private void createVolumeCommodities(final CommoditiesBoughtFromProvider commBoughtGrouping,
                    ShoppingListTO shoppingList, TopologyEntityDTO.Builder volume) {
        // Add commodity bought list
        CommoditiesBoughtFromProvider.Builder commBoughtListBuilder =
                CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                        .setProviderId(commBoughtGrouping.getProviderId());
        for (CommodityBoughtDTO comm : commBoughtGrouping.getCommodityBoughtList()) {
            if (OLD_QUANTITY_REQUIRED_COMM_TYPES.contains(comm.getCommodityType().getType())) {
                CommodityBoughtDTO.Builder commBoughtBuilder = CommodityBoughtDTO.newBuilder()
                        .setCommodityType(comm.getCommodityType());
                if (comm.getCommodityType().getType() == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE) {
                    // Storage amount unit was converted to GB when creating traderTO.
                    // Convert the unit back to MB when creating the projected value.
                    // Cost and API expects storage to be in MB.
                    commBoughtBuilder.setUsed(comm.getUsed() * Units.KIBI);
                } else {
                    commBoughtBuilder.setUsed(comm.getUsed());
                }
                commBoughtListBuilder.addCommodityBought(commBoughtBuilder);
            }
        }
        volume.addCommoditiesBoughtFromProviders(commBoughtListBuilder);

        // Get the adjusted storage amount and storage access values assigned by the market.
        Map<Integer, Float> commTypeToAssignedCapacity = new HashMap<>();
        for (final CommodityBoughtTO commodityBoughtTO : shoppingList.getCommoditiesBoughtList()) {
            commTypeToAssignedCapacity.put(commodityBoughtTO.getSpecification().getBaseType(), commodityBoughtTO.getAssignedCapacityForBuyer());
        }
        // Add commodity sold list
        for (CommodityBoughtDTO comm : commBoughtGrouping.getCommodityBoughtList()) {
            if (OLD_QUANTITY_REQUIRED_COMM_TYPES.contains(comm.getCommodityType().getType())) {
                CommoditySoldDTO.Builder commSoldBuilder = CommoditySoldDTO.newBuilder()
                        .setCommodityType(comm.getCommodityType());
                if (comm.getCommodityType().getType() == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE) {
                    Float assignedCapacity = commTypeToAssignedCapacity.get(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE);
                    if (assignedCapacity != null) {
                        commSoldBuilder.setCapacity(assignedCapacity * Units.KIBI);
                    }
                } else if (comm.getCommodityType().getType() == CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE) {
                    Float assignedCapacity = commTypeToAssignedCapacity.get(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE);
                    if (assignedCapacity != null) {
                        commSoldBuilder.setCapacity(assignedCapacity);
                    }
                } else {
                    commSoldBuilder.setCapacity(comm.getUsed());
                }
                volume.addCommoditySoldList(commSoldBuilder);
            }
        }
    }

    private Optional<ConnectedEntity> createConnectedAzOrRegion(
        final TopologyEntityDTOOrBuilder topologyEntityDTO) {
        final List<TopologyEntityDTO> azOrRegions = TopologyDTOUtil.getConnectedEntitiesOfType(
            topologyEntityDTO,
            Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE),
            entityOidToDto);
        return azOrRegions.stream().findFirst()
            .map(azOrRegion -> ConnectedEntity.newBuilder()
                .setConnectedEntityId(azOrRegion.getOid())
                .setConnectedEntityType(azOrRegion.getEntityType())
                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                .build());
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
        final boolean originalCloudConsumerExists = originalCloudConsumer != null;
        if (originalCloudConsumerExists) {
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
                    TopologyEntityDTO sourceRegion = originalCloudConsumerExists
                            ? cloudTc.getRegionOfCloudConsumer(originalCloudConsumer)
                            : null;
                    EconomyDTOs.Context contextWithRegionPresent = cloudTc.getContextWithRegionPresent(traderTO);
                    Long regionCommSpec = null;
                    if (Objects.nonNull(contextWithRegionPresent)) {
                        regionCommSpec = contextWithRegionPresent.getRegionId();
                    }
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
                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION).build();
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
     * @param traderOidToEntityDTO Map from traderTO OID to original TopologyEntityDTO.
     * @return list of {@link TopologyDTO.CommoditySoldDTO}s
     */
    private Set<TopologyDTO.CommoditySoldDTO> retrieveCommSoldList(
            @Nonnull final TraderTO traderTO,
            @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> traderOidToEntityDTO) {

        // First we merge all timeslot commdities
        final Map<CommodityType, List<Double>> timeSlotsByCommType = Maps.newHashMap();
        final Collection<CommodityDTOs.CommoditySoldTO.Builder> commBuilders =
            traderTO.getCommoditiesSoldList().stream().map(CommoditySoldTO::toBuilder)
                .collect(Collectors.toList());
        final Collection<CommoditySoldTO> mergedCommodities =
            mergeTimeSlotCommodities(commBuilders,
                timeSlotsByCommType,
                CommoditySoldTO.Builder::getSpecification,
                CommoditySoldTO.Builder::getQuantity,
                CommoditySoldTO.Builder::getPeakQuantity,
                CommoditySoldTO.Builder::setQuantity,
                CommoditySoldTO.Builder::setPeakQuantity)
                .stream().map(CommoditySoldTO.Builder::build).collect(Collectors.toList());
        return mergedCommodities.stream()
                    .map(commoditySoldTO -> commSoldTOtoCommSoldDTO(traderTO, commoditySoldTO,
                        timeSlotsByCommType, traderOidToEntityDTO))
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
     * @return an array of two elements, the first element is an array of new used values,
     * the second is the array of new peak values
     */
    @VisibleForTesting
    protected float[][] getResizedCapacity(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO,
            @Nonnull final TopologyDTO.CommodityBoughtDTO commBought,
            @Nullable final Long providerOid) {

        logger.debug("Recalculating new capacity for {}", topologyDTO.getDisplayName());
        Optional<float[]> histUsedValue =
                CommodityConverter.getHistoricalUsedOrPeak(commBought,
                        TopologyDTO.CommodityBoughtDTO::getHistoricalUsed);
        Optional<float[]> histPeakValue =
                CommodityConverter.getHistoricalUsedOrPeak(commBought,
                        TopologyDTO.CommodityBoughtDTO::getHistoricalPeak);
        if (logger.isDebugEnabled()) {
            checkHistValues(commBought, histUsedValue, histPeakValue);
        }
        // if no historical values retrieved, use real time values
        if (!histUsedValue.isPresent()) {
            logger.debug("Using current used value {} for recalculating resize capacity for {}",
                    commBought.getUsed(), commBought.getCommodityType().getType());
            histUsedValue = Optional.of(new float[]{(float)commBought.getUsed()});
        }
        if (!histPeakValue.isPresent()) {
            logger.debug("Using current peak value {} for recalculating resize capacity for {}",
                    commBought.getPeak(), commBought.getCommodityType().getType());
            histPeakValue = Optional.of(new float[]{(float)commBought.getPeak()});
        }

        float[] histUsed = histUsedValue.orElseGet(() -> new float[] {});
        float[] histPeak = histPeakValue.orElseGet(() -> new float[] {});

        if (topologyDTO.getEnvironmentType() != EnvironmentType.CLOUD
                || !CLOUD_SCALING_ENTITY_TYPES.contains(topologyDTO.getEntityType())) {
            final float[][] standardResizedCapacity = getStandardResizedCapacity(histUsed, histPeak,
                commBought, providerOid, topologyDTO);
            return new float[][]{standardResizedCapacity[0], standardResizedCapacity[1]};
        }

        // Handle entities for which we generate cloud-scaling actions.
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
                if (providerOid != null) {
                    return drivingSoldCommodityBasedCapacity(drivingCommmoditySoldList, topologyDTO,
                            providerOid, commBought);
                }
            }
        }
        if (providerOid != null
                && TopologyConversionConstants.CLOUD_BOUGHT_COMMODITIES_RESIZED.contains(
                commBought.getCommodityType().getType())) {
            final MarketTier marketTier = cloudTc.getMarketTier(providerOid);
            if (marketTier != null) {
                TopologyEntityDTO tier = null;
                if (marketTier.hasRIDiscount()) {
                    // If the VM is on an RI, then we cannot just do MarketTier.getTier() as that
                    // will represent the biggest tier in that family for ISF RIs. Instead get the
                    // actual compute tier provider.
                    Optional<TopologyEntityDTO> computeTier = cloudTopology.getComputeTier(topologyDTO.getOid());
                    if (computeTier.isPresent()) {
                        tier = computeTier.get();
                    } else {
                        logger.error("Cannot find compute tier for {}", topologyDTO.getOid());
                        return new float[][]{histUsed, histPeak};
                    }
                } else {
                    tier = marketTier.getTier();
                }
                final Optional<CommoditySoldDTO> commoditySoldDTO = tier
                        .getCommoditySoldListList()
                        .stream()
                        .filter(c -> c.getCommodityType().equals(commBought.getCommodityType()))
                        .filter(CommoditySoldDTO::hasCapacity)
                        .findFirst();

                if (commoditySoldDTO.isPresent() && commoditySoldDTO.get().getIsResizeable()
                        && commoditySoldDTO.get().hasCapacity()) {
                    return getCloudResizableBoughtCommodityCapacity(topologyDTO, commBought, histUsed,
                        histPeak, commoditySoldDTO.get(), providerOid);
                } else {
                    logger.debug("Tier {} does not sell commodity type {} for entity {}",
                            tier::getDisplayName, commBought::getCommodityType,
                            topologyDTO::getDisplayName);
                }
            }
        }
        return new float[][]{histUsed, histPeak};
    }

    /**
     * Calculate capacity for bought commodities in cloud entity which is resizable.
     *
     * @param topologyDTO cloud entity {@link TopologyEntityDTO}
     * @param commBought {@link CommodityBoughtDTO} of the cloud entity which its provider
     * @param histUsed historical used values
     * @param histPeak historical peak values
     * @param commoditySoldDTO the corresponding {@link CommoditySoldDTO}
     * @return an array of two elements, the first element is an array of new used values,
     *         the second is the array of new peak values
     */
    private float[][] getCloudResizableBoughtCommodityCapacity(@Nonnull final TopologyEntityDTO topologyDTO,
                                                               @Nonnull final CommodityBoughtDTO commBought,
                                                               float[] histUsed,
                                                               final float[] histPeak,
                                                               final CommoditySoldDTO commoditySoldDTO,
                                                               final long providerOid) {

        if (commBought.hasHistoricalUsed() && commBought.getHistoricalUsed().hasPercentile()) {
            float targetUtil = (float)commBought.getResizeTargetUtilization();
            if (targetUtil <= 0.0) {
                targetUtil = EntitySettingSpecs.UtilTarget.getSettingSpec()
                        .getNumericSettingValueType()
                        .getDefault() / 100;
            }
            // For cloud entities / cloud migration plans, we do not use the peakQuantity.
            // We only use the quantity values inside M2
            final float percentile = (float)commBought.getHistoricalUsed().getPercentile();

            logger.debug("Using percentile value {} to recalculate capacity for {} in {}",
                    percentile, commBought.getCommodityType().getType(), topologyDTO.getDisplayName());

            float histUsage = percentile * (float)commoditySoldDTO.getCapacity();

            final float resizeUsage = compareAndResizeCapacityWithCapacityLimits(topologyDTO,
                    commBought.getCommodityType(), commBought.getReservedCapacity(),
                    (histUsage / targetUtil), commoditySoldDTO.getCapacity());
            commoditiesResizeTracker.save(topologyDTO.getOid(), providerOid, commBought.getCommodityType(),
                    resizeUsage - commoditySoldDTO.getCapacity() > 0, CommodityLookupType.PROVIDER);
            return new float[][]{new float[]{resizeUsage}, new float[]{resizeUsage}};
        } else {
            // We want to use the historical used (already smoothened) for both the resize-up
            // and resize-down demand calculations. We do not want to consider the
            // historical max or the peaks to avoid a one-time historic max value to cause
            // resize decisions.
            final float[] resizedQuantity = calculateResizedQuantity(topologyDTO, histUsed[0],
                    (float)commBought.getUsed(), histPeak[0],
                    (float)commoditySoldDTO.getCapacity(),
                    (float)commBought.getResizeTargetUtilization(), false, commBought, Optional.empty());
            commoditiesResizeTracker.save(topologyDTO.getOid(), providerOid, commBought.getCommodityType(),
                    resizedQuantity[0] - commoditySoldDTO.getCapacity() > 0, CommodityLookupType.PROVIDER);
            logger.debug("Using a peak used of {} for commodity type {} for entity {}.",
                    resizedQuantity[1], commBought.getCommodityType().getType(),
                    topologyDTO.getDisplayName());
            return new float[][]{new float[]{resizedQuantity[0]},
                    new float[]{resizedQuantity[1]}};
        }
    }

    private float[][] drivingSoldCommodityBasedCapacity(final List<CommoditySoldDTO> drivingCommmoditySoldList,
                                                        final TopologyEntityDTO topologyDTO, long providerOid,
                                                        final CommodityBoughtDTO commBought) {
        final CommoditySoldDTO commoditySoldDTO = drivingCommmoditySoldList.get(0);
        final float drivingCommSoldCapacity = (float)commoditySoldDTO.getCapacity();
        // Cloud migration lift and shift plan (a.k.a. allocation plan) uses the capacity
        // value of the commodity of drivingCommSold.
        // The optimization plan (a.k.a consumption plan) will consider commodity usage and target
        // utilization when determining the resize quantity.
        // If commodity has resizable set to false, resize quantity should be same as current
        // capacity.
        final boolean useCurrentCapacity =
            !commoditySoldDTO.getIsResizeable() || isCloudMigration && !isCloudResizeEnabled;
        if (useCurrentCapacity) {
            logger.debug("Returning current capacity of sold commodity as quantity: {}, for "
                + "commodity type: {}, entity oid: {}, isCloudMigration: {}, "
                    + "isCloudResizeEnabled: {}, is commodity resizable: {}",
                drivingCommSoldCapacity, commoditySoldDTO.getCommodityType().getType(),
                topologyDTO.getOid(), isCloudMigration, isCloudResizeEnabled,
                commoditySoldDTO.getIsResizeable());
            return new float[][]{new float[]{drivingCommSoldCapacity},
                    new float[]{drivingCommSoldCapacity}};
        }
        final float histUsage;
        Optional<Float> demand = extractDemandFromCommSold(commoditySoldDTO, topologyDTO);
        if (demand.isPresent()) {
            histUsage = demand.get();
        } else {
            logger.debug("Returning current capacity of sold commodity as quantity: {}, for "
                            + "commodity type: {}, entity oid: {} has no used, historical used (percentile or "
                            + "hist utilization) found.", drivingCommSoldCapacity,
                    commoditySoldDTO.getCommodityType().getType(), topologyDTO.getOid());
            return new float[][]{new float[]{drivingCommSoldCapacity},
                    new float[]{drivingCommSoldCapacity}};
        }
        final EntityType entityType = EntityType.forNumber(topologyDTO.getEntityType());
        final boolean useTargetUtilBand = EntitySettingSpecs.TargetBand.getEntityTypeScope()
            .contains(entityType);
        final float[] resizedQuantity =
                calculateResizedQuantity(topologyDTO, histUsage,
                        (float)commoditySoldDTO.getUsed(), (float)commoditySoldDTO.getPeak(),
                        (float)commoditySoldDTO.getCapacity(),
                        (float)commoditySoldDTO.getResizeTargetUtilization(), useTargetUtilBand, commBought, Optional.of(commoditySoldDTO));
        commoditiesResizeTracker.save(topologyDTO.getOid(), providerOid, commoditySoldDTO.getCommodityType(),
                resizedQuantity[0] - drivingCommSoldCapacity > 0, CommodityLookupType.CONSUMER);
        logger.debug(
                "Using quantity {}, peak quantity {}, targetUtilBand: {} for driving commodity "
                        + "type {} for entity {}, with current capacity: {}",
                resizedQuantity[0], resizedQuantity[1], useTargetUtilBand,
                commoditySoldDTO.getCommodityType().getType(), topologyDTO.getDisplayName(),
            drivingCommmoditySoldList);
        return new float[][]{new float[]{resizedQuantity[0]},
                new float[]{resizedQuantity[1]}};
    }

    private Optional<Float> extractDemandFromCommSold(CommoditySoldDTO commoditySoldDTO,
                                                           TopologyEntityDTO topologyDTO
                                                           ) {
        final float drivingCommSoldCapacity = (float)commoditySoldDTO.getCapacity();
        final float histUsage;
        if (commoditySoldDTO.hasHistoricalUsed()
            && (commoditySoldDTO.getHistoricalUsed().hasPercentile()
            || commoditySoldDTO.getHistoricalUsed().hasHistUtilization())) {
            if (commoditySoldDTO.getHistoricalUsed().hasPercentile()
                && commoditySoldDTO.hasCapacity()) {
                final float percentile = (float)commoditySoldDTO.getHistoricalUsed().getPercentile();
                logger.debug("Using percentile value {} to recalculate capacity for {} in {}",
                    percentile, commoditySoldDTO.getCommodityType().getType(),
                    topologyDTO.getDisplayName());
                histUsage = percentile * drivingCommSoldCapacity;
            } else {
                // if not then hist utilization which is the historical used value.
                histUsage =
                    (float)commoditySoldDTO.getHistoricalUsed().getHistUtilization();
                logger.debug("Using historical value {} to recalculate capacity for {} in {}",
                    histUsage, commoditySoldDTO.getCommodityType().getType(),
                    topologyDTO.getDisplayName());
            }
            return Optional.of(histUsage);
        } else if (commoditySoldDTO.hasUsed()) {
            // If for a commodity (eg: DBS -> storage amount) we do not have either percentile or historical usage,
            // we resize based on used values itself. If used values are also NA, we use capacity value.
            histUsage = (float)commoditySoldDTO.getUsed();
            return Optional.of(histUsage);
        }
        return Optional.empty();
    }

    /**
     * Determines if we need to prevent resize of {@code optCommoditySolByConsumer} based on
     * raw material usage.
     * We prevent resize of the commoditySoldByConsumer in 2 cases:
     * 1. CommoditySoldByConsumer is congested AND any of the raw materials is also highly utilized.
     * 2. CommoditySoldByConsumer is under utilized AND any of the raw materials is also under utilized.
     *
     * To check if a raw material is congested or under-utilized, we compare its historical usage against effective capacity.
     *
     * @param topologyDTO the entity which is being resized.
     * @param optCommoditySolByConsumer commodity sold by consumer.
     * @param resizeDemand the resize demand.
     * @param targetUtil the target util.
     * @param targetBandRadius the target band radius.
     * @param useTargetUtilBand determines if the commodity uses targetUtilBand for resizing.
     * @return true if the resize needs to be prevented. False otherwise.
     */
    private boolean doesRawMaterialUsagePreventResize(final TopologyEntityDTO topologyDTO,
                                                      final Optional<CommoditySoldDTO> optCommoditySolByConsumer,
                                                      final float resizeDemand,
                                                      final float targetUtil,
                                                      final float targetBandRadius,
                                                      final boolean useTargetUtilBand) {
        if (!optCommoditySolByConsumer.isPresent()) {
            return false;
        }
        CommoditySoldDTO commoditySoldDTO = optCommoditySolByConsumer.get();
        RawMaterialInfo rawMaterialInfo = rawMaterialsMap.get(commoditySoldDTO.getCommodityType().getType());
        if (Objects.isNull(rawMaterialInfo)) {
            return false;
        }
        Set<Integer> rawMaterials = rawMaterialInfo.getRawMaterials().stream()
            .map(rm -> rm.getRawMaterial()).collect(Collectors.toSet());
        if (rawMaterials.isEmpty()) {
            return false;
        }
        float radius = useTargetUtilBand ? targetBandRadius : 0;
        boolean isCongested = resizeDemand / commoditySoldDTO.getCapacity() > targetUtil + radius;
        boolean isUnderUtilized = resizeDemand / commoditySoldDTO.getCapacity() < targetUtil - radius;
        if (!isCongested && !isUnderUtilized) {
            return false;
        }
        Set<CommoditySoldDTO> soldRawMaterials =
            topologyDTO.getCommoditySoldListList().stream()
                    .filter(c -> c.getCommodityType().getType() != commoditySoldDTO.getCommodityType().getType()
                            && rawMaterials.contains(c.getCommodityType().getType()))
                .collect(Collectors.toSet());
        for (CommoditySoldDTO soldRawMaterial : soldRawMaterials) {
            if (!soldRawMaterial.hasEffectiveCapacityPercentage()) {
                continue;
            }
            Optional<Float> demand = extractDemandFromCommSold(soldRawMaterial, topologyDTO);
            if (!demand.isPresent()) {
                continue;
            }
            float usage = demand.get();
            float effectiveCapacity = (float)(soldRawMaterial.getEffectiveCapacityPercentage() / 100 * soldRawMaterial.getCapacity());
            if (isCongested && usage >= effectiveCapacity || isUnderUtilized && usage < effectiveCapacity) {
                return true;
            }
        }
        return false;
    }

    /**
     * Warn if the number of historical used quantities is different from the number of historical
     * peak values, and the difference is legitimately unexpected.
     *
     * @param commBought commodity bought which values to check
     * @param histUsed historical used values
     * @param histPeak historical peak values
     */
    private static void checkHistValues(
        @Nonnull final TopologyDTO.CommodityBoughtDTO commBought,
        @Nonnull final Optional<float[]> histUsed,
        @Nonnull final Optional<float[]> histPeak) {
        if (histUsed.isPresent() && histPeak.isPresent()) {
            final float[] usedQuantities = histUsed.get();
            final float[] peakQuantities = histPeak.get();
            if (usedQuantities.length != peakQuantities.length) {
                final int histUsedTimeslots = commBought.hasHistoricalUsed()
                    ? commBought.getHistoricalUsed().getTimeSlotCount() : 0;
                final int histPeakTimeslots = commBought.hasHistoricalPeak()
                    ? commBought.getHistoricalPeak().getTimeSlotCount() : 0;
                // if one set of historical values has timeslots while the other one doesn't,
                // the difference would be expected
                if (histUsedTimeslots == histPeakTimeslots && peakQuantities.length > 0) {
                    logger.warn("Different lengths of used and peak quantities for commodity {}. "
                            + "Received {} used and {} peak values. ",
                        commBought::getCommodityType, () -> usedQuantities.length,
                        () -> peakQuantities.length);
                }
            }
        }
    }

    /**
     * Calculates the new resized  capacities for the bought commodity based on historical
     * demands.
     *
     * @param used Bought commodity historical used value.
     * @param peak Bought commodity peak value.
     * @param commBought Bought commodity.
     * @param providerOid Oid of the provider DTO for this commodity.
     * @param topologyDTO the TopologyDTO buying this commodity.
     * @return the new calculated capacities for the bought commodity.
     */
    private float[][] getStandardResizedCapacity(final float[] used,
                                                 final float[] peak,
                                                 @Nonnull final TopologyDTO.CommodityBoughtDTO commBought,
                                                 final Long providerOid,
                                                 @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO) {
        // An example use case for the condition below is the
        // VDI use case where we need to apply the target Util on the percentile.
        final TopologyEntityDTO providerTopologyEntity = entityOidToDto.get(providerOid);
        if (providerTopologyEntity == null) {
            logger.warn("Could not find provider for entity {} with id {}." +
                            " Using percentile used and peak without " +
                            "applying the target utilization", topologyDTO.getDisplayName(),
                    providerOid);
            return new float[][]{used, peak};
        }
        float histUsed = used[0];
        float histPeak = peak[0];
        // Find the corresponding sold commodity. For example, in the VDI use case,
        // this would be the image commodity sold by the desktop pool.
        final List<TopologyDTO.CommoditySoldDTO> commoditiesSoldByProvider =
                providerTopologyEntity.getCommoditySoldListList()
                        .stream()
                        .filter(c -> c.getCommodityType().getType()
                                == commBought.getCommodityType().getType())
                        .collect(Collectors.toList());
        if (commoditiesSoldByProvider.size() != 1 || commoditiesSoldByProvider.get(0) == null) {
            logger.debug("Could not find corresponding sold commodity from provider {} for "
                    + "type {}, for entity {}. Using percentile used and peak without "
                    + "applying the target utilization", providerTopologyEntity.getDisplayName(),
                    commBought.getCommodityType().getType(), topologyDTO.getDisplayName());
            return new float[][]{new float[]{histUsed}, new float[]{histPeak}};
        } else {
            float targetUtil = (float)commBought.getResizeTargetUtilization();
            double capacity = commoditiesSoldByProvider.get(0).getCapacity();
            histUsed = (float)(histUsed / targetUtil);
            histPeak = (float)(histPeak / targetUtil);
            //If commBought has percentile, the histUsed is in percentage
            if (commBought.hasHistoricalUsed() && commBought.getHistoricalUsed().hasPercentile()) {
                histPeak *= capacity;
                histUsed *= capacity;
            }
            histUsed = compareAndResizeCapacityWithCapacityLimits(topologyDTO, commBought.getCommodityType(),
                    commBought.getReservedCapacity(), histUsed, capacity);
            logger.debug("New capacity for entity {} 's commodity {}: {}, providerId: {}, targetUtil:{}, old capacity: {}",
                    topologyDTO.getDisplayName(), commBought.getCommodityType().getType(),
                    histUsed, providerTopologyEntity.getDisplayName(), targetUtil, capacity);
            if (TopologyConversionConstants.ONPREM_BOUGHT_COMMODITIES_TO_TRACK.contains(commBought.getCommodityType().getType())) {
                    commoditiesResizeTracker.save(topologyDTO.getOid(), providerOid, commBought.getCommodityType(),
                        histUsed > capacity, CommodityLookupType.PROVIDER);
            }
        }
        if (commBought.hasHistoricalUsed() && commBought.getHistoricalUsed().hasPercentile()) {
            return new float[][]{new float[]{histUsed}, new float[]{histPeak}};
        }
        return new float[][]{used, peak};
    }

    private float[] calculateResizedQuantity(final TopologyEntityDTO topologyDTO, float resizeDemand,
                                             float used, float peak, float capacity,
                                             float targetUtil,
                                             boolean useTargetUtilBand,
                                             final CommodityBoughtDTO commodityBoughtDTO,
                                             final Optional<CommoditySoldDTO> commoditySoldDTO) {
        if (targetUtil <= 0.0) {
            targetUtil = EntitySettingSpecs.UtilTarget.getSettingSpec()
                    .getNumericSettingValueType()
                    .getDefault() / 100;
        }
        // targetBandRadius is a delta around target utilization. If resizeDemand / capacity, i.e.
        //  desired utilization (a value between [0, 1]) is within targetUtil + targetBandRadius
        // and targetUtil - targetBandRadius, then the resize quantity should be the same as
        // current capacity. Otherwise calculate resize quantity as resizeDemand / targetUtil.
        final float targetBandRadius =
                EntitySettingSpecs.TargetBand.getSettingSpec().getNumericSettingValueType()
                        .getDefault() / 200;
        final boolean resizeDemandOutsideTargetBand = outsideTargetBand(targetUtil, resizeDemand,
            capacity, targetBandRadius);
        // For cloud entities / cloud migration plans, we do not use the peakQuantity.
        // We only use the quantity values inside M2
        final float peakQuantity = Math.max(used, peak) / targetUtil;
        final float quantity;
        if ((!useTargetUtilBand || resizeDemandOutsideTargetBand) &&
            !doesRawMaterialUsagePreventResize(topologyDTO, commoditySoldDTO, resizeDemand,
                targetUtil, targetBandRadius, useTargetUtilBand)) {
            quantity = resizeDemand / targetUtil;
        } else {
            quantity = capacity;
        }
        final float quantityWithCapacityLimits = compareAndResizeCapacityWithCapacityLimits(topologyDTO,
                commodityBoughtDTO.getCommodityType(), commodityBoughtDTO.getReservedCapacity(), quantity, capacity);
        return new float[] {quantityWithCapacityLimits, peakQuantity};
    }

    /**
     * Compares resized values against reserved capacity values for a commodity.
     * Also compares to lower_bound_scale_up in case of scaling up. if the new {@param resizedCapacity}
     * is outside the bounds, we reset to either {@param reservedCapacity} or to {@link CommodityCapacityLimit}.
     *
     * @param topologyDTO      current topologyDTO.
     * @param commodityType    commodity type for context.
     * @param reservedCapacity commBought.getReservedCapacity defaults to 0.
     * @param resizedCapacity  recommended capacity based on utilization target for the commodity.
     * @param currentCapacity  current used to determine if the commodity is scaling up or down.
     * @return new recommended capacity.
     */
    private float compareAndResizeCapacityWithCapacityLimits(final TopologyEntityDTO topologyDTO,
                                                             final CommodityType commodityType,
                                                             final double reservedCapacity,
                                                             final float resizedCapacity,
                                                             final double currentCapacity) {
        if (resizedCapacity < reservedCapacity) {
            logger.debug("Entity : {}, {} commodity's resized value {} does not meet reserved capacity." +
                    "Setting it to reserved capacity : {}", topologyDTO.getDisplayName(),
                    commodityType, resizedCapacity, reservedCapacity );
            return (float)reservedCapacity;
        }
        if (resizedCapacity > currentCapacity // check if the commodity is scaling up. LowerBoundScaleUp applies only while scaling up.
                && topologyDTO.hasTypeSpecificInfo() && topologyDTO.getTypeSpecificInfo().hasDatabase()
                && !topologyDTO.getTypeSpecificInfo().getDatabase().getLowerBoundScaleUpList().isEmpty()) {
            Optional<CommodityCapacityLimit> commodityCapacityLimit = topologyDTO
                    .getTypeSpecificInfo().getDatabase().getLowerBoundScaleUpList()
                    .stream().filter(commodityWithLimit -> commodityWithLimit.hasCommodityType()
                            && commodityWithLimit.getCommodityType() == commodityType.getType()).findFirst();
            if (commodityCapacityLimit.isPresent()
                    && commodityCapacityLimit.get().hasCapacity()
                    && commodityCapacityLimit.get().getCapacity() > resizedCapacity) {
                logger.debug("Entity : {} -> {} commodity's" +
                                "scaling up but does not meet LowerBoundScaleUp value." +
                                "Setting resized value from {} to LowerBoundScaleUp value : {}", topologyDTO.getDisplayName(),
                        commodityType, resizedCapacity, commodityCapacityLimit.get().getCapacity());
                return commodityCapacityLimit.get().getCapacity();
            }
        }
        return resizedCapacity;
    }

    private static boolean outsideTargetBand(final float targetUtilization, final float demandValue,
                                    final float capacity, final float delta) {
        return demandValue / capacity > (targetUtilization + delta)
            || demandValue / capacity < (targetUtilization - delta);
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
     * {@link CommodityBoughtDTO}
     * @param reservedCapacityResults the reserved capacity information
     * @param originalEntity the original entity DTO
     * @param timeSlotsByCommType Timeslot values arranged by {@link CommodityBoughtTO}
     * @param isProvisioned Whether this trader is a provisioned trader which doesn't have shoppinglist.
     * @return {@link TopologyDTO.CommoditySoldDTO} that the trader sells
     */
    @Nonnull
    private Optional<TopologyDTO.CommodityBoughtDTO> commBoughtTOtoCommBoughtDTO(
            final long traderOid, final long supplierOid, final long slOid,
            @Nonnull final CommodityBoughtTO commBoughtTO,
            @Nonnull final ReservedCapacityResults reservedCapacityResults,
            final TopologyEntityDTO originalEntity,
            @Nonnull Map<CommodityType, List<Double>> timeSlotsByCommType,
            boolean isProvisioned) {


        float peak = commBoughtTO.getPeakQuantity();
        if (peak < 0) {
            conversionErrorCounts.recordError(ErrorCategory.PEAK_NEGATIVE,
                commBoughtTO.getSpecification().getDebugInfoNeverUseInCode());
            logger.trace("The entity with negative peak is {} (buying {} from {})",
                traderOid, commBoughtTO, supplierOid);
            peak = 0;
        }

        final float peakQuantity = peak; // It must be final

        final ShoppingListInfo shoppingListInfo = shoppingListOidToInfos.get(slOid);
        Optional<CommodityType> commodityTypeOpt = commodityConverter.marketToTopologyCommodity(commBoughtTO.getSpecification());
        // Remove CommodityType DataCenter for entities with providers from list of CLOUD_ENTITY_TYPES_TO_CREATE_DC_COMM_BOUGHT
        // before creating CommBoughtDTO. These additional commodityBoughtTOs were introduced in convertToMarket
        // see: #createDCCommodityBoughtForCloudEntity.
        if (shoppingListInfo != null
                && shoppingListInfo.getSellerEntityType().isPresent()
                && CLOUD_ENTITY_TYPES_TO_CREATE_DC_COMM_BOUGHT.contains(shoppingListInfo.getSellerEntityType().get())
                && commodityTypeOpt.isPresent()
                && commodityTypeOpt.get().getType() == CommodityDTO.CommodityType.DATACENTER_VALUE) {
            return Optional.empty();
        }
        return commodityTypeOpt.map(commType -> {
                    Optional<CommodityBoughtDTO> boughtDTObyTraderFromProjectedSellerInRealTopology =
                        getCommodityIndex().getCommBought(traderOid, supplierOid, commType);
                    double currentUsage = getOriginalUsedValue(commBoughtTO, traderOid,
                            supplierOid, commType, originalEntity);
                    currentUsage = TopologyConversionUtils.convertMarketUnitToTopologyUnit(
                            commType.getType(), currentUsage, originalEntity, isCloudMigration);
                    final Builder builder = CommodityBoughtDTO.newBuilder();
                    builder.setUsed(reverseScaleComm(currentUsage, boughtDTObyTraderFromProjectedSellerInRealTopology,
                                    CommodityBoughtDTO::getScalingFactor))
                    .setReservedCapacity(
                            reservedCapacityResults.getReservedCapacity(traderOid, commType))
                    .setCommodityType(commType)
                    .setPeak(reverseScaleComm(peakQuantity, boughtDTObyTraderFromProjectedSellerInRealTopology,
                            CommodityBoughtDTO::getScalingFactor));

                    boughtDTObyTraderFromProjectedSellerInRealTopology.map(CommodityBoughtDTO::getScalingFactor)
                                    .ifPresent(builder::setScalingFactor);

                    /*
                       if (bought by buyer from projected seller commodity in real topology exists) {
                           there are several possible use cases here:
                                1) Seller didn't change after work of market.
                                   It's either resize or no action at all
                                2) Seller changed after market's work.
                                   It is moving some entity to entity1 -> entity2 -> entity1
                           just use percentile from original topology
                       } else {
                            Buyer didn't buy from projectedSeller in real topology.
                            It is definitely a move action (seller changed).
                            we should calculate projected percentile value and use it
                       }
                     */
                    //A provisioned trader doesn't have shopping list info and boughtCommodities
                    //so don't populate projected percentile for it.
                    if (!isProvisioned) {
                        final Double projectedPercentile =
                                    boughtDTObyTraderFromProjectedSellerInRealTopology
                                                    .map(CommodityBoughtDTO::getHistoricalUsed)
                                                    .filter(HistoricalValues::hasPercentile)
                                                    .map(HistoricalValues::getPercentile)
                                                    .orElse(getProjectedPercentileValue(supplierOid,
                                                                                        shoppingListInfo,
                                                                                        commType));
                        if (projectedPercentile != null) {
                            builder.setHistoricalUsed(HistoricalValues.newBuilder().setPercentile(projectedPercentile).build());
                        }
                    }

                    // Set timeslot values if applies
                    if (timeSlotsByCommType.containsKey(commType)) {
                        if (builder.hasHistoricalUsed()) {
                            builder.setHistoricalUsed(
                                builder.getHistoricalUsed().toBuilder()
                                    .addAllTimeSlot(timeSlotsByCommType.get(commType))
                                    .build());
                        } else {
                            builder.setHistoricalUsed(HistoricalValues.newBuilder()
                                .addAllTimeSlot(timeSlotsByCommType.get(commType))
                                .build());
                        }
                    }

                    return builder.build(); });
    }

    /**
     * Calculates projected percentile if it is possible.
     *
     * @implNote projectedPercentile == originalPercentile * oldCapacity / newCapacity
     *
     * @param supplierOid oid of provider in projected topology (seller)
     * @param shoppingListInfo information about commodities that are bought in real topology.
     * @param commType commodity type
     *
     * @return projected percentile value or null if percentile is missing in original commodity
     * or it's not possible to calculate it
     */
    private Double getProjectedPercentileValue(long supplierOid, ShoppingListInfo shoppingListInfo,
                                               CommodityType commType) {
        if (shoppingListInfo == null || shoppingListInfo.getSellerId() == null) {
            logger.debug("Shopping list info is null or sellerId is null. Unable to calculate projected percentile. Shopping list is {}",
                         shoppingListInfo);
            return null;
        }

        final List<CommodityBoughtDTO> commodityBoughtDTOs =
                        shoppingListInfo.getCommodities().stream()
                                        .filter(commodity -> commType
                                                        .equals(commodity.getCommodityType()))
                                        .collect(Collectors.toList());
        if (commodityBoughtDTOs.size() != 1) {
            logger.warn("There are too many or too few (count is {} but expected is 1) commodities with type {} that sold by supplier {} to buyer {}. Shopping list is {}.",
                        commodityBoughtDTOs.size(),
                        commType,
                        supplierOid,
                        shoppingListInfo.getBuyerId(),
                        shoppingListInfo);
            return null;
        }

        CommodityBoughtDTO boughtDTO = commodityBoughtDTOs.get(0);
        if (boughtDTO == null || boughtDTO.getHistoricalUsed() == null || !boughtDTO.getHistoricalUsed().hasPercentile()) {
            return null;
        }

        final double originalPercentile = boughtDTO.getHistoricalUsed().getPercentile();
        long oldSupplierId = shoppingListInfo.getSellerId();
        long newSupplierId = supplierOid;
        if (shoppingListInfo.getSellerEntityType().isPresent() && TopologyDTOUtil.isTierEntityType(shoppingListInfo.getSellerEntityType().get())) {
            // If it is tier type, the traderOid is stored in the shoppinglistInfo.
            // Need to convert it to the entityOid in order to look up in commodityIndex
            if (cloudTc.isMarketTier(oldSupplierId)) {
                final MarketTier marketTier = cloudTc.getMarketTier(oldSupplierId);
                if (marketTier.hasRIDiscount()) {
                    // For now the system only support RI for computeTier.  In the further if other tiers
                    // can be RI, this needs to be handled in a generic way.
                    final Optional<TopologyEntityDTO> computeTier = cloudTopology.getComputeTier(shoppingListInfo.getBuyerId());
                    if (computeTier.isPresent()) {
                        oldSupplierId = computeTier.get().getOid();
                    } else {
                        logger.error("{} does not have compute tier supplier", shoppingListInfo.getBuyerId());
                        oldSupplierId = marketTier.getTier().getOid();
                    }
                } else {
                    oldSupplierId = cloudTc.getMarketTier(oldSupplierId).getTier().getOid();
                }
            }
            if (cloudTc.isMarketTier(newSupplierId)) {
                // new supplier is always OnDemandMarketTier
                newSupplierId = cloudTc.getMarketTier(newSupplierId).getTier().getOid();
            }
        }
        final Optional<Double> oldCapacity = getCommodityIndex().getCommSold(
            oldSupplierId,
            commType).map(CommoditySoldDTO::getCapacity);
        final Optional<Double> newCapacity =
            getCommodityIndex().getCommSold(newSupplierId, commType)
                                        .map(CommoditySoldDTO::getCapacity)
                                        .filter(capacity -> capacity != 0);
        if (oldCapacity.isPresent() && newCapacity.isPresent()) {
            logger.debug("converting for commType={}, buyer oid {}, seller {} oid={}.  oldCapacity={}, newcapacity={}, originalPercentile={}, newPercentile={}",
                commType, shoppingListInfo.getBuyerId(), shoppingListInfo.getSellerEntityType(), shoppingListInfo.getSellerId(), oldCapacity, newCapacity, originalPercentile, originalPercentile * oldCapacity.get() / newCapacity.get());
            return originalPercentile * oldCapacity.get() / newCapacity.get();
        }

        logger.warn("Projected percentile approximation can't be calculated. commType={}; Original percentile = {}, oldCapacity = {}, newCapacity = {}, boughtDTO = {}",
            commType,
            originalPercentile,
            oldCapacity,
            newCapacity,
            boughtDTO);
        return null;
    }

    /**
     * Find the original used value for a given Bought commodity TO.
     * @param commBoughtTO - BoughtTO for which we want the original used value.
     * @param traderOid Trader Oid
     * @param supplierOid Provider Oid
     * @param commType commodity type
     * @param originalEntity the original topology entity DTO for the trader.
     * @return the used value
     */
    private float getOriginalUsedValue(final CommodityBoughtTO commBoughtTO, final long traderOid,
                                       final long supplierOid, final CommodityType commType,
                                       @Nullable final TopologyEntityDTO originalEntity) {
        float currentUsage = commBoughtTO.getQuantity();
        // If this is a cloud entity, find the original bought commodity from supplier.
        if (originalEntity != null && cloudTc.isMarketTier(supplierOid)) {
            if (cloudTc.getMarketTier(supplierOid).getTier() != null &&
                    TopologyConversionConstants.CLOUD_BOUGHT_COMMODITIES_RESIZED
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
                        getCommodityIndex().getCommBought(traderOid,
                            providerDTO.stream().findFirst().get().getOid(),
                            commType);
                    if (originalCommodityBoughtDTO.isPresent()) {
                        currentUsage = (float)originalCommodityBoughtDTO.get().getUsed();
                    }
                } else if (providerDTO.isEmpty()) {
                    logger.warn("{} has no provider of type {} in the topology.",
                        originalEntity.getDisplayName(), EntityType.forNumber(providerType));
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
             * 1. Topology settings - policy sends the suspendable flag
             * (entity settings and analysis settings (happens later))
             * 2. Topology sends controllable flag
             * 3. Whether entity is a VM in plan (to improve)
             * 4. Whether in plan the VM hosts containers (in which case 3 is overridden)
             * 5. Whether entity is top of supply chain (to improve)
             * 6. Whether it is a DB or DBServer on cloud
             */
            // If topologyEntity set suspendable value, we should directly use it.
            boolean suspendable = topologyDTO.getAnalysisSettings().hasSuspendable()
                            ? topologyDTO.getAnalysisSettings().getSuspendable()
                            : EntitySettings.BooleanKey.ENABLE_SUSPEND.value(topologyDTO);
            boolean isProviderMustClone = EntitySettings.BooleanKey
                    .PROVIDER_MUST_CLONE.value(topologyDTO);
            // If the entity hosts containers, use the entity setting.  Otherwise, force VM
            // suspension to false. In the future, mediation needs to set the suspend/clone setting
            // so that this override is not required.
            if (!providersOfContainers.contains(topologyDTO.getOid())) {
                if ((bottomOfSupplyChain && active) ||
                        topOfSupplyChain ||   // Workaround for OM-25254. Should be set by mediation
                        entityType == EntityType.VIRTUAL_MACHINE_VALUE) {
                    suspendable = false;
                }
            }
            // If topologyEntity set clonable value, we should directly use it.
            clonable = topologyDTO.getAnalysisSettings().hasCloneable()
                            ? topologyDTO.getAnalysisSettings().getCloneable()
                                            : clonable;
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

            if (!controllable && uncontrollableEntityToMessage.containsKey(topologyDTO.getEntityType())) {
                logUncontrollable(uncontrollableEntityToMessage.get(topologyDTO.getEntityType()), topologyDTO, null);
            }

            final boolean reconfigurable = topologyDTO.getAnalysisSettings().hasReconfigurable()
                && topologyDTO.getAnalysisSettings().getReconfigurable()
                && EntitySettings.BooleanKey.ENABLE_RECONFIGURE.value(topologyDTO);

            boolean isEntityFromCloud = TopologyConversionUtils.isEntityConsumingCloud(topologyDTO);
            TraderSettingsTO.Builder settingsBuilder = TopologyConversionUtils
                    .createCommonTraderSettingsTOBuilder(topologyDTO, unmodifiableEntityOidToDtoMap);
            settingsBuilder.setClonable(clonable && controllable)
                    .setReconfigurable(reconfigurable && controllable)
                    .setControllable(controllable)
                    // cloud providers do not come here. We will hence be setting this to true just for
                    // on-prem storages
                    .setCanSimulateAction(entityType == EntityType.STORAGE_VALUE)
                    .setSuspendable(suspendable && controllable)
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
                    .setProviderMustClone(isProviderMustClone)
                    .setDaemon(topologyDTO.getAnalysisSettings().getDaemon())
                    .setRateOfResize(topologyDTO.getAnalysisSettings().getRateOfResize())
                    .setConsistentScalingFactor(topologyDTO.getAnalysisSettings().getConsistentScalingFactor())
                    .setMinReplicas(topologyDTO.getAnalysisSettings().getMinReplicas())
                    .setMaxReplicas(topologyDTO.getAnalysisSettings().getMaxReplicas());

            final float rateOfResize = topologyDTO.getAnalysisSettings().getRateOfResize();
            if (rateOfResizeTranslationMap.containsKey(rateOfResize)) {
                settingsBuilder.setRateOfResize(rateOfResizeTranslationMap.get(rateOfResize));
            } else {
                settingsBuilder.setRateOfResize(rateOfResizeTranslationMap.get(MEDIUM_RATE_OF_RESIZE));
            }

            // Overwrite flags for vSAN
            if (TopologyConversionUtils.isVsanStorage(topologyDTO) ||
                    topologyDTO.getEntityType() == EntityType.CLUSTER_VALUE) {
                settingsBuilder.setGuaranteedBuyer(true).setClonable(false)
                    .setSuspendable(false).setResizeThroughSupplier(true);
            }

            //compute biclique IDs for this entity, the clique list will be used only for
            // shop together placement, so pmBasedBicliquer is called
            Set<Long> allCliques = pmBasedBicliquer.getBcIDs(String.valueOf(topologyDTO.getOid()));

            Pair<List<EconomyDTOs.ShoppingListTO>, Long> p =
                    createAllShoppingListsAndReturnProviderId(topologyDTO);
            List<EconomyDTOs.ShoppingListTO> shoppingLists = p.getFirst();
            TraderTO.Builder traderDTOBuilder = TraderTO.newBuilder()
                    // Type and Oid are the same in the topology DTOs and economy DTOs
                    .setOid(topologyDTO.getOid())
                    .setType(entityType)
                    .setState(state)
                    .setDebugInfoNeverUseInCode(entityDebugInfo(topologyDTO))
                    .addAllCommoditiesSold(createAllCommoditySoldTO(topologyDTO))
                    .addAllShoppingLists(shoppingLists)
                    .addAllCliques(allCliques);
            createCurrentContext(topologyDTO, p.getSecond())
                .ifPresent(settingsBuilder::setCurrentContext);
            consistentScalingHelper.getScalingGroupId(topologyDTO.getOid())
                .ifPresent(traderDTOBuilder::setScalingGroupId);
            traderDTOBuilder.setSettings(settingsBuilder);
            traderDTO = traderDTOBuilder.build();
        } catch (Exception e) {
            logger.error(EconomyConstants.EXCEPTION_MESSAGE, entityDebugInfo(topologyDTO),
                    e.getMessage(), e);
        }
        return traderDTO;
    }

    private Optional<Context.Builder> createCurrentContext(final TopologyEntityDTO topologyDTO,
                                                           final Long providerId) {
        final TopologyEntityDTO businessAccount = cloudEntityToBusinessAccount.get(topologyDTO);
        if (businessAccount == null || isCloudMigration) {
            // For cloud migration case, we want it to find the cheapest among the
            // destination regions, rather than using its current BA.
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
                    .setTotalAllocatedCoupons(TopologyConversionUtils.getTotalNumberOfCouponsCovered(riCoverage.get()))
                    .build();
            result.addFamilyBasedCoverage(entry);
        }
        return Optional.of(result);
    }

    private double getTotalNumberOfCouponsRequested(TopologyEntityDTO entity) {
        return entity.getCommoditiesBoughtFromProvidersList().stream().filter(CommoditiesBoughtFromProvider::hasProviderId)
                .filter(entry -> entry.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE)
                .map(entry -> entityOidToDto.get(entry.getProviderId()))
                .map(TopologyEntityDTO::getTypeSpecificInfo)
                .map(TopologyDTO.TypeSpecificInfo::getComputeTier)
                .mapToDouble(TopologyDTO.TypeSpecificInfo.ComputeTierInfo::getNumCoupons)
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
        // Set the account pricing data oid on the balance account. If it is not found,
        // have the VM shop for its own business account id.
        final Optional<Long> priceId = cloudTc
                .getAccountPricingIdFromBusinessAccount(businessAccount.getOid())
                .map(AccountPricingData::getAccountPricingDataOid);
        final BalanceAccountDTO.Builder balanceAccount = BalanceAccountDTO.newBuilder()
                .setBudget(defaultBudgetValue)
                .setSpent(spent)
                .setPriceId(priceId.orElse(businessAccount.getOid()))
                .setId(businessAccount.getOid());
        // Set Billing Family ID if present
        if (cloudTopology != null) { // can be null in unit tests
            cloudTopology.getBillingFamilyForEntity(businessAccount.getOid())
                    .map(GroupAndMembers::group)
                    .map(Grouping::getId)
                    .ifPresent(balanceAccount::setParentId);
        }
        return balanceAccount.build();
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
        Map<Long, Long> providers = TopologyDTOUtil.parseOldProvidersMap(topologyEntity, gson);
        List<ShoppingListTO> shoppingLists = new ArrayList<>();
        Long computeTierProviderId = null;
        // Sort the commBoughtGroupings based on provider type and oid so
        // that the input into analysis is consistent every cycle
        // 1. First based on provider type (largest provider type first). So this way, for VMs,
        //    the PM SL appears first followed by the Storage SLs. And we can use this fact in SNM
        //    for a performance gain. We can ignore the simulation of the first and last SLs
        //    (the PM SL and the last storage SL).
        // 2. And lastly, by providerId. This was needed for VSAN DataStores, which have multiple PM SLs.
        //    To consistently order these, we sort by the providerId of the SL.
        List<CommoditiesBoughtFromProvider> sortedCommBoughtGrouping = topologyEntity.getCommoditiesBoughtFromProvidersList().stream()
                .sorted(Comparator.comparing(CommoditiesBoughtFromProvider::getProviderEntityType).reversed()
                                .thenComparing(CommoditiesBoughtFromProvider::getProviderId))
                .collect(Collectors.toList());

        // Additional commBought needs to be added to shopping list.
        final Map<Long, CommoditiesBoughtFromProvider> additionalCommBought = sortedCommBoughtGrouping.stream()
            .filter(commBoughtGrouping -> shouldMoveCommodity(topologyEntity, commBoughtGrouping))
            .collect(Collectors.toMap(CommoditiesBoughtFromProvider::getProviderId, Function.identity()));

        // We want the input into M2 to be consistent across cycles. So, we sort the commBoughtGroupings.
        for (TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider commBoughtGrouping : sortedCommBoughtGrouping) {
            /*
              skip converting shoppinglist that buys from VDC
              skip converting shoppinglist that buys VDC commodities only
              (e.g. VM buys VDC only commodity from DesktopPool)
             */
            // TODO: we also skip the sl that consumes AZ which contains Zone commodity because zonal RI is not yet supported
            final boolean hasNonVdcCommodities =
                            commBoughtGrouping.getCommodityBoughtList().stream()
                                            .map(CommodityBoughtDTO::getCommodityType)
                                            .map(CommodityType::getType)
                                            .noneMatch(MarketAnalysisUtils.VDC_COMMODITY_TYPES::contains);
            if (includeByType(commBoughtGrouping.getProviderEntityType())
                    && commBoughtGrouping.getProviderEntityType() != EntityType.AVAILABILITY_ZONE_VALUE
                    && hasNonVdcCommodities) {
                TopologyEntityDTO entityForSL = topologyEntity;
                CommoditiesBoughtFromProvider commBoughtGroupingForSL = commBoughtGrouping;
                final Set<Integer> providerTypeAfterCollapsing = CollapsedTraderHelper
                        .getNewProviderTypeAfterCollapsing(topologyEntity.getEntityType(),
                                commBoughtGrouping.getProviderEntityType());
                if (providerTypeAfterCollapsing != null && !providerTypeAfterCollapsing.isEmpty()) {
                    Long directProviderId = commBoughtGrouping.getProviderId();
                    TopologyEntityDTO directProvider = entityOidToDto.get(directProviderId);
                    if (directProvider == null) {
                        logger.error("Provider {} for entity {} not found", directProviderId, topologyEntity.getOid());
                        continue;
                    }
                    commBoughtGroupingForSL = directProvider.getCommoditiesBoughtFromProvidersList().stream()
                            .filter(CommoditiesBoughtFromProvider::hasProviderEntityType)
                            .filter(commBought -> providerTypeAfterCollapsing.contains(commBought.getProviderEntityType()))
                            .findAny().orElse(null);
                    if (commBoughtGroupingForSL == null) {
                        // When directProviderId is an on prem volume constructed from classic model,
                        // it has empty bought list so commBoughtGroupingForSL is expected to be null.
                        // When directProviderId is an on prem volume constructed from new model or
                        // directProvider is a cloud volume, the commBoughtGroupingForSL will be the
                        // volume's commBoughtGroupingForSL that consumes on a storage or a storage tier.
                        if (!directProvider.getCommoditySoldListList().isEmpty()
                                || !directProvider.getCommoditiesBoughtFromProvidersList().isEmpty()) {
                            logger.error("Couldn't find commBoughtGrouping with provider type {} for"
                                    + " collapsed entity {}", providerTypeAfterCollapsing, directProviderId);
                        }
                        continue;
                    }
                    entityForSL = directProvider;
                }
                Pair<Long, Class> providerId;
                if (scalingGroupUsage.isPresent() && commBoughtGroupingForSL.getProviderEntityType()
                        == EntityType.COMPUTE_TIER_VALUE) {
                    providerId = scalingGroupUsage.get().getProviderId(entityForSL.getOid());
                } else {
                    providerId = getProviderId(commBoughtGroupingForSL, entityForSL);
                }
                if (providerId.getSecond() == RiDiscountedMarketTier.class) {
                    computeTierProviderId = providerId.getFirst();
                }
                if (skipShoppingListCreation(entityForSL, commBoughtGrouping)) {
                    logger.trace("For context: {}: Skip SL creation for: {} ({}), entity: {}.",
                            topologyInfo.getTopologyContextId(), entityForSL.getDisplayName(),
                            entityForSL.getOid(), topologyEntity.getDisplayName());
                    continue;
                }
                if (createSpecialShoppingList(entityForSL, commBoughtGrouping, shoppingLists)) {
                    continue;
                }
                shoppingLists.add(createGenericShoppingList(entityForSL, topologyEntity, entityForSL.getEntityType(),
                            topologyEntity.getAnalysisSettings().getShopTogether(), providerId.getFirst(),
                            // Pass scalingGroupUsage down to underlying methods only when creating shoppingList
                            // for topologyEntity. If shoppingList is created for entityForSL(which is different
                            // from topologyEntity), no need to use scalingGroupUsage which is for topologyEntity.
                            commBoughtGroupingForSL, providers,
                            entityForSL == topologyEntity ? scalingGroupUsage : Optional.empty(),
                            additionalCommBought.getOrDefault(providerId.getFirst(), CommoditiesBoughtFromProvider.getDefaultInstance())));
            }
        }
        return new Pair<>(shoppingLists, computeTierProviderId);
    }

    /**
     * Check if commodities of commBoughtGrouping should be added to another shopping list.
     * If entity is VM, provider type is storage and all commodities of commBoughtGrouping are access commodities, then return true.
     *
     * @param topologyEntity topology entity
     * @param commBoughtGrouping commodity bought of entity
     * @return true if entity is VM, provider type is storage and all commodities of commBoughtGrouping are access commodities
     */
    private boolean shouldMoveCommodity(@Nonnull final TopologyEntityDTO topologyEntity,
                                        @Nonnull final CommoditiesBoughtFromProvider commBoughtGrouping) {
        return topologyEntity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
            && commBoughtGrouping.getProviderEntityType() == EntityType.STORAGE_VALUE
            && ACCESS_COMMODITY_TYPES.containsAll(
            commBoughtGrouping.getCommodityBoughtList().stream()
                .map(commBought -> commBought.getCommodityType().getType()).collect(Collectors.toSet()));
    }

    /**
     * Checks if shopping list creation needs to be skipped - e.g. in case of
     * 1) ephemeral local volumes that are transient;
     * 2) container pod's storage shopping list, which is yet to be supported in plan;
     * 3) it is a "shouldMove" commodity.
     *
     * @param entityDto Entity DTO for which shopping list creation needs to be checked.
     * @param commBought commodity bought of entity
     * @return True if shopping list creation will be skipped for the entity.
     */
    @VisibleForTesting
    boolean skipShoppingListCreation(@Nonnull final TopologyEntityDTO entityDto,
                                     @Nonnull final CommoditiesBoughtFromProvider commBought) {
        if (entityDto.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE
                && entityDto.getTypeSpecificInfo().getTypeCase() == TypeCase.VIRTUAL_VOLUME) {
            final VirtualVolumeInfo volumeInfo = entityDto.getTypeSpecificInfo().getVirtualVolume();
            return volumeInfo.hasIsEphemeral() && volumeInfo.getIsEphemeral();
        }
        if (isPlan() && entityDto.getEntityType() == EntityType.CONTAINER_POD_VALUE
                && commBought.hasProviderEntityType()
                && commBought.getProviderEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
            // Container pod's storage shopping list isn't supported in plan.
            // See OM-78687 for this interim solution to allow pods to be placed.
            // See OM-78778 for a proper solution which will take time to design and implement.
            return true;
        }
        return shouldMoveCommodity(entityDto, commBought);
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
                            topologyEntity.getDisplayName(), providerId.getFirst(), providerId.getSecond().getSimpleName());
                    }
                    for (CommodityBoughtDTO commBought : commBoughtGrouping.getCommodityBoughtList()) {
                        if (!commBought.getActive() || !consistentScalingHelper
                                .isCommodityConsistentlyScalable(commBought.getCommodityType())) {
                            continue;
                        }
                        final List<UsedAndPeak> boughtQuantities =
                                getCommBoughtQuantities(topologyEntity,
                                commBought, providerId.getFirst());
                        usage.addUsage(commBought, boughtQuantities.get(0).used);
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
     * A utility function to create a shopping list based on special circumstances that cannot be
     * generalized.
     *
     * @param entityDto Entity DTO for which shopping list creation needs to be checked.
     * @param commBought commodity bought of entity
     * @return true if the special circumstance has been handled
     */
    @VisibleForTesting
    boolean createSpecialShoppingList(
            @Nonnull final TopologyEntityDTO entityDto,
            @Nonnull final CommoditiesBoughtFromProvider commBought,
            @Nonnull final List<EconomyDTOs.ShoppingListTO> shoppingLists) {
        if (entityDto.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                && commBought.hasProviderEntityType()
                && commBought.getProviderEntityType() == EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE) {
            createShoppingListBetweenVMAndCntCluster(entityDto, commBought.getProviderId(), commBought)
                    .ifPresent(shoppingLists::add);
            return true;
        }
        return false;
    }

    /**
     * A utility function to create a shopping list between VM and ContainerCluster to force
     * Market to generate a Reconfigure action for a k8s node that is in a NotReady status.
     * When a k8s node becomes NotReady, kubeturbo probe does the following:
     *   - Let the node buy a keyed CLUSTER commodity with a provider of CONTAINER_PLATFORM_CLUSTER
     *     entity type.
     *   - In the meantime, the CONTAINER_PLATFORM_CLUSTER entity does not sell the same CLUSTER
     *     commodity that the node wants to buy.
     * This utility function identifies this scenario, and creates a movable shopping list for the
     * VM such that Market will run a placement algorithm for it and generate a Reconfigure action
     * because of the mismatch of commodities.
     *
     * @param buyer the buyer of the shopping list
     * @param providerId the provider ID
     * @param commBoughtGroupingForSL the shopping list to check
     * @return an optional {@link ShoppingListTO}
     */
    private Optional<EconomyDTOs.ShoppingListTO> createShoppingListBetweenVMAndCntCluster(
            @Nonnull final TopologyEntityDTO buyer,
            @Nonnull final Long providerId,
            @Nonnull final CommoditiesBoughtFromProvider commBoughtGroupingForSL) {
        return commBoughtGroupingForSL.getCommodityBoughtList().stream()
                .filter(commBought -> commBought.getCommodityType().getType() ==
                        CommodityDTO.CommodityType.CLUSTER_VALUE)
                .findAny()
                .map(commBoughtDTO -> {
                    final long id = shoppingListId++;
                    return EconomyDTOs.ShoppingListTO
                            .newBuilder()
                            .setOid(id)
                            .setSupplier(providerId)
                            .addAllCommoditiesBought(createAndValidateCommBoughtTO(
                                    buyer, commBoughtDTO, providerId, Optional.empty()))
                            .setMovable(true)
                            .build();
                });
    }

    /**
     * Create a shopping list for a specified buyer and the entity it is buying from.
     *
     * @param entityForSL topology entity used to create shoppingList
     * @param originalEntityAsTrader topology entity as trader
     * @param entityType the entity type of entityForSL
     * @param shopTogether whether the entity supports the shop-together feature
     * @param providerOid the oid of the seller of the shopping list
     * @param commBoughtGroupingForSL commoditiesBought for entityForSL from provider with providerOid
     * @param providers a map that captures the previous placement for unplaced plan entities
     * @param scalingGroupUsage cached usage data for buyers in scaling groups
     * @param additionalCommBought Additional commBought needs to be added to shopping list
     * @return a shopping list between the buyer and seller
     */
    @Nonnull
    private EconomyDTOs.ShoppingListTO createGenericShoppingList(
            final TopologyEntityDTO entityForSL,
            final TopologyEntityDTO originalEntityAsTrader,
            final int entityType,
            final boolean shopTogether,
            @Nullable final Long providerOid,
            @Nonnull final CommoditiesBoughtFromProvider commBoughtGroupingForSL,
            final Map<Long, Long> providers,
            final Optional<ScalingGroupUsage> scalingGroupUsage,
            @Nonnull final CommoditiesBoughtFromProvider additionalCommBought) {
        long entityForSLOid = entityForSL.getOid();
        TopologyDTO.TopologyEntityDTO provider = (providerOid != null) ? entityOidToDto.get(providerOid) : null;
        final float moveCost = calculateMoveCost(entityForSL, entityType, provider, commBoughtGroupingForSL);
        final List<CommodityBoughtDTO> allCommBought;
        if (additionalCommBought.getCommodityBoughtCount() == 0) {
            allCommBought = commBoughtGroupingForSL.getCommodityBoughtList();
        } else {
            allCommBought = new ArrayList<>(commBoughtGroupingForSL.getCommodityBoughtList());
            allCommBought.addAll(additionalCommBought.getCommodityBoughtList());
        }
        final Set<CommodityDTOs.CommodityBoughtTO> values = filterUnknownLicense(allCommBought, entityForSL)
            .filter(CommodityBoughtDTO::getActive)
            .map(topoCommBought -> convertCommodityBought(entityForSL, topoCommBought, providerOid,
                shopTogether, providers, scalingGroupUsage))
            // Null for DSPMAccess/Datastore and shop-together
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .collect(Collectors.toSet());
        boolean addGroupFactor = false;
        if (cloudTc.isMarketTier(providerOid)) {
            // For cloud buyers, add biClique comm bought because we skip them in
            // convertCommodityBought method
            if (!shopTogether) {
                // NOTE: Skip biClique creation for migration case where the shoptogether is true
                // for migrating entities.
                values.addAll(createBcCommodityBoughtForCloudEntity(providerOid, entityForSLOid));
            }
            // Create DC comm bought
            if (!isCloudMigration) {
                TopologyEntityDTO marketTier = cloudTc.getMarketTier(providerOid).getTier();
                Optional<CommodityBoughtTO> regionComm;
                if (EntityType.COMPUTE_TIER_VALUE == marketTier.getEntityType() && marketTier
                        .getConnectedEntityListList().stream().anyMatch(ce -> ce.getConnectedEntityType()
                                == EntityType.AVAILABILITY_ZONE_VALUE)) {
                    // Skip creating DC comm representing the regional availability for GCP compute
                    // tiers as GCP directly rely on connectedEntityList to achieve zonal availability.
                    regionComm = Optional.empty();
                } else {
                    // Having DC commodity prevents us from going from one CSP to another for migration.
                    // AWS and Azure reaches here and regional access is done by DC comm.
                    regionComm = createDCCommodityBoughtForCloudEntity(providerOid, entityForSLOid);
                }
                regionComm.ifPresent(values::add);
            }
            if (!isSMAOnly() || isCloudMigration) {
                // Create Coupon Comm
                Optional<CommodityBoughtTO> coupon = createCouponCommodityBoughtForCloudEntity(
                        providerOid, entityForSLOid);
                if (coupon.isPresent()) {
                    values.add(coupon.get());
                    addGroupFactor = true;
                }
            }
            // Create template exclusion commodity bought
            values.addAll(createTierExclusionCommodityBoughtForCloudEntity(providerOid, entityForSLOid));
        } else if (isCloudMigration) {
            values.addAll(createTierExclusionCommodityBoughtForMigratingEntity(providerOid, entityForSL));
        }
        final long id = shoppingListId++;
        // Check if the provider of the shopping list is UNKNOWN. If true, set movable false.
        final boolean isProviderUnknownOrFailover = skippedEntities.containsKey(providerOid) &&
                (skippedEntities.get(providerOid).getEntityState() == TopologyDTO.EntityState.UNKNOWN
                || skippedEntities.get(providerOid).getEntityState() == EntityState.FAILOVER);
        if (isProviderUnknownOrFailover) {
            logger.debug("Making movable false for shoppingList of entity {} which has provider " +
                "{} in UNKNOWN/FAILOVER state",
                entityForSL::getDisplayName, () -> skippedEntities.get(providerOid).getDisplayName());
        }
        // For Migrate to Cloud it doesn't matter if the current provider is not healthy
        boolean isMovable = !isProviderUnknownOrFailover
            // Containers cannot move off their ContainerPods
            && !(provider != null && provider.getEntityType() == EntityType.CONTAINER_POD_VALUE)
            && (commBoughtGroupingForSL.hasMovable()
                ? commBoughtGroupingForSL.getMovable()
                : AnalysisUtil.MOVABLE_TYPES.contains(entityType));
        if (TopologyConversionUtils.isVsanStorage(entityForSL) ||
            entityForSL.getEntityType() == EntityType.CLUSTER_VALUE) {
            isMovable = false;
        }

        if (entityForSL.getEnvironmentType() == EnvironmentType.CLOUD
                && CLOUD_SCALING_ENTITY_TYPES.contains(entityForSL.getEntityType())) {
            // Apply scalable from mediation to movable for cloud VMs.
            isMovable &= commBoughtGroupingForSL.getScalable();
            // Apply EligibleForScale to movable for cloud VMs in realtime
            if (!isPlan()) {
                // In realtime analysis, we need to check if the cost notification status has succeeded.
                // If it hasn't, we mark movable false for cloud VMs.
                isMovable = isMovable && entityForSL.getAnalysisSettings().getIsEligibleForScale()
                        && costNotificationStatus == Status.SUCCESS;
            }
        }

        // if the buyer of the shopping list is in control state(controllable = false), or if the
        // shopping list has a provider and the provider is in control state (controllable = false)
        // the shopping list should not move
        final boolean isControllable = entityForSL.getAnalysisSettings()
                        .getControllable() && (provider == null || (provider != null &&
                        provider.getAnalysisSettings().getControllable()));
        isMovable = isMovable && isControllable;

        final boolean isCloudVM = IS_CLOUD_VM.test(commBoughtGroupingForSL.getProviderEntityType(), entityType);
        //SMA is not supported for Migrate to Cloud plan
        final boolean addShoppingListToSMA = MarketMode.isEnableSMA(marketMode) && !isCloudMigration
                && includeByType(commBoughtGroupingForSL.getProviderEntityType())
                && isCloudVM;

        //SMA doesn't care about isMovable, we only use it to fill cloudVmComputeShoppingListIDs
        //if isMovable==false, we won't add SHoppingListTo to this set and VM will remains on the
        //same template in SMA
        if (addShoppingListToSMA && isMovable) {
            cloudVmComputeShoppingListIDs.add(id);
        }

        if (isCloudVM) {
            // Turn off movable for cloud scaling group members that are not group leaders.
            isMovable &= addGroupFactor && consistentScalingHelper.getGroupFactor(entityForSL) > 0;
        }

        if (!isMovable && unmovableEntityToMessage.containsKey(entityType)) {
            logUncontrollable(unmovableEntityToMessage.get(entityType), entityForSL, provider);
        }

        final EconomyDTOs.ShoppingListTO.Builder economyShoppingListBuilder = EconomyDTOs.ShoppingListTO
                .newBuilder()
                .setOid(id)
                .setStorageMoveCost(moveCost)
                .setMovable(isMovable);
        if (providerOid != null) {
            economyShoppingListBuilder.setSupplier(providerOid);
        }
        if (addGroupFactor) {
            // Since we added a coupon commodity, the group factor will be required in order to
            // obtain a price quote.  Set it to zero here, just to make the group factor present
            // in the SL. The actual group factor will be set in the scaling group leader after
            // the proper value is known.
            economyShoppingListBuilder
                .setGroupFactor(consistentScalingHelper.getGroupFactor(entityForSL));
        }
        final Integer providerEntityType = commBoughtGroupingForSL.hasProviderEntityType()
                ? commBoughtGroupingForSL.getProviderEntityType() : null;
        // multiple on-prem per-disk volumes placed on the same storage are still shopping together (as vm)
        Set<Long> resourceIds = getRelatedVolumesOnProvider(entityForSL,
                        commBoughtGroupingForSL.getProviderId());
        String resourceDisplayName = null;
        if (isCloudMigration && !resourceIds.isEmpty()) {
            // for MCP choose any of them from current provider for referencing in SL
            // in the projected topology only one volume will be created for possibly multiple source
            if (resourceIds.size() > 1) {
                resourceDisplayName = resourceIds.stream().map(entityOidToDto::get)
                                .filter(Objects::nonNull).map(TopologyEntityDTO::getDisplayName).sorted()
                                .collect(Collectors.joining(", "));
            }
            resourceIds = Collections.singleton(resourceIds.iterator().next());
        }
        // Preserve collapsedBuyerId in ShoppingListInfo. With new on prem volume model (VM -> VV- > ST),
        // collapsedBuyerId(volume id) is preserved in shopping list.
        final Long collapsedBuyerId = entityForSL == originalEntityAsTrader ? null : entityForSLOid;
        shoppingListOidToInfos.put(id, new ShoppingListInfo(id, originalEntityAsTrader.getOid(),
                        providerOid, resourceIds, resourceDisplayName, collapsedBuyerId,
                        providerEntityType, allCommBought));

        // in SMAOnly mode we are preventing M2 to generate actions for cloud VMs
        if (addShoppingListToSMA && isSMAOnly()) {
            economyShoppingListBuilder.setMovable(false);
        }

        if (entityType == EntityType.VIRTUAL_VOLUME_VALUE
                && originalEntityAsTrader.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                || isCloudMigration && (entityType == EntityType.VIRTUAL_VOLUME_VALUE
                || entityType == EntityType.VIRTUAL_MACHINE_VALUE)) {
            // Set cloud volume shoppingList to be in Savings/Reversibility mode.
            final boolean isDemandScalable = isCloudMigration || !isReversibilityPreferred(entityForSLOid);
            economyShoppingListBuilder.setDemandScalable(isDemandScalable);
            // Modify values set if applicable.
            dropIopsDemandForThroughputDrivenVolume(entityForSL, values);
        }
        economyShoppingListBuilder.addAllCommoditiesBought(values);

        // Add unquoted commodities, if needed.
        // This functionality is used to enable the placement of active on-prem VMs on providers
        // that are over-provisioned (OM-63941).
        if (shouldAddUnquotedProvisionedCommodities(entityForSL, commBoughtGroupingForSL)) {
            economyShoppingListBuilder.addUnquotedCommoditiesBaseTypeList(
                    CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE);
            economyShoppingListBuilder.addUnquotedCommoditiesBaseTypeList(
                    CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE);
        }

        return economyShoppingListBuilder.build();
    }

    /**
     * Check if the entity's shopping list should have unquoted commodities.
     * The unquoted commodities are added in 2 cases:
     * 1. Compute SLs of active on-prem VMs in real-time topology - controlled
     *    by {@link GlobalSettingSpecs#AllowUnlimitedHostOverprovisioning}
     * 2. Cluster SLs of on-prem VMs. Added workload will not get the unquoted
     *    commodities because they are unplaced when coming into market. Added workload should get
     *    infinite quote if cluster is full.
     * @param entity the entity for which the shopping list should have unquoted provisioned commodities.
     * @param commBoughtGroupingForSL the commodity bought grouping
     * @return true if unquoted provisioned commodities should be added, false otherwise.
     */
    private boolean shouldAddUnquotedProvisionedCommodities(TopologyEntityDTO entity,
                                                            CommoditiesBoughtFromProvider commBoughtGroupingForSL) {
        return (unquotedCommoditiesEnabled
                && entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                && entity.getEntityState() == TopologyDTO.EntityState.POWERED_ON
                && entity.getEnvironmentType() != EnvironmentType.CLOUD
                && commBoughtGroupingForSL.getProviderEntityType() == EntityType.PHYSICAL_MACHINE_VALUE
                && !isCloudMigration
                && !isPlan())
                ||
                (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                && commBoughtGroupingForSL.hasProviderId()
                && commBoughtGroupingForSL.getProviderEntityType() == EntityType.CLUSTER_VALUE
                && entity.getEnvironmentType() == EnvironmentType.ON_PREM
                && fakeEntityCreator.isFakeComputeClusterOid(commBoughtGroupingForSL.getProviderId()));
    }

    /**
     * If the {@link GlobalSettingSpecs#AllowUnlimitedHostOverprovisioning} setting is disabled, then we want
     * to enable move actions to overprovisioned providers by adding unquoted commodities.
     *
     * @param analysisConfig the market config, which contains the market global settings.
     * @return true if the setting is disabled, false otherwise.
     */
    private boolean isUnquotedCommoditiesEnabled(@Nonnull final AnalysisConfig analysisConfig) {
        Optional<Setting> enableUnquotedCommoditiesSetting =
                analysisConfig.getGlobalSetting(GlobalSettingSpecs.AllowUnlimitedHostOverprovisioning);
        return enableUnquotedCommoditiesSetting
                .map(setting -> setting.getBooleanSettingValue().getValue())
                .orElse(false);
    }


    /**
     * For throughput driven volumes, drop IOPS demand for analysis.
     * A volume is throughput driven if it meets the following formula --
     * (ThroughputUsed / IOPSUsed) > (MaxThroughput / (MaxIOPS * minimum achievable IOPS percentage)),
     * which means the average block size transferred to/from the volume is larger than desirable block
     * size for current tier. We need to drop IOPS demand in this case because --
     * 1. For large block size case, throughput demand is the driving factor for analysis.
     * 2. IOPS is based on 16KiB data blocks for SSD tiers, and 1MiB data blocks for HDD tiers. A SSD
     * volume with large block size can have relatively high IOPS demand because the data blocks are
     * capped by 16KiB, and if placed on a HDD tier, may not need that many of IOPS.
     * For example, a volume on SSD with block size 256KiB and 1000IOPS, if placed on HDD tier,
     * can be 256 * 2000 / 1024 = 250 IOPS with 1MiB block size.
     *
     * @param cloudVolume cloud volume to check
     * @param boughtTOS volume shoppingList boughtTOs.
     */
    private void dropIopsDemandForThroughputDrivenVolume(final TopologyEntityDTO cloudVolume,
                                                         final Set<CommodityDTOs.CommodityBoughtTO> boughtTOS) {
        CommodityBoughtTO iopsBoughtTO = boughtTOS.stream().filter(b ->
                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE == b.getSpecification().getBaseType())
                .findFirst().orElse(null);
        CommodityBoughtTO throughputBoughtTO = boughtTOS.stream().filter(b ->
                CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE == b.getSpecification().getBaseType())
                .findFirst().orElse(null);
        if (iopsBoughtTO == null || throughputBoughtTO == null
                || iopsBoughtTO.getQuantity() == 0f || iopsBoughtTO.getAssignedCapacityForBuyer() == 0f) {
            return;
        }
        if (throughputBoughtTO.getQuantity() / iopsBoughtTO.getQuantity()
                > throughputBoughtTO.getAssignedCapacityForBuyer()
                / (iopsBoughtTO.getAssignedCapacityForBuyer() * MINIMUM_ACHIEVABLE_IOPS_PERCENTAGE)) {
            logger.info("Dropping IOPS quantity {} for cloud volume {} with oid {}",
                    iopsBoughtTO.getQuantity(), cloudVolume.getDisplayName(), cloudVolume.getOid());
            boughtTOS.remove(iopsBoughtTO);
            boughtTOS.add(iopsBoughtTO.toBuilder().setQuantity(0f).build());
        }
    }

    /**
     * Creates a DC Comm bought for a cloud entity which has a provider as a Compute tier,
     * DatabaseTier or DatabaseServerTier.
     * These additional CommBoughtTOs are removed in convertFromMarket before creating CommBoughtDTOs
     * as these commodities are not found on their seller.
     * see {@link #commBoughtTOtoCommBoughtDTO}.
     *
     * @param providerOid oid of the market tier provider oid
     * @param buyerOid    oid of the buyer of the shopping list
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
            final CommoditySpecificationTO commoditySpecs =
                    commodityConverter.commoditySpecification(dcCommSold.getCommodityType());
            dcCommBought = CommodityDTOs.CommodityBoughtTO.newBuilder()
                    .setSpecification(commoditySpecs)
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
    public Optional<CommodityBoughtTO> createCouponCommodityBoughtForCloudEntity(
            long providerOid, long buyerOid) {
        MarketTier marketTier = cloudTc.getMarketTier(providerOid);
        int providerEntityType = marketTier.getTier().getEntityType();
        CommodityBoughtTO couponCommBought = null;
        if (providerEntityType == EntityType.COMPUTE_TIER_VALUE) {
            Optional<EntityReservedInstanceCoverage> riCoverage = cloudTc.getRiCoverageForEntity(buyerOid);
            float couponQuantity = 0;
            if (riCoverage.isPresent()) {
                couponQuantity = TopologyConversionUtils.getTotalNumberOfCouponsCovered(riCoverage.get());
            }
            final CommoditySpecificationTO commoditySpecs = commodityConverter.commoditySpecification(
                    CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.COUPON_VALUE)
                            .build());
            couponCommBought = CommodityBoughtTO.newBuilder()
                    .setSpecification(commoditySpecs)
                    .setPeakQuantity((float)entityOidToDto.get(marketTier.getTier().getOid())
                            .getTypeSpecificInfo()
                            .getComputeTier()
                            .getNumCoupons())
                    .setQuantity(couponQuantity)
                    .build();
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
    @Nonnull
    private Set<CommodityBoughtTO> createTierExclusionCommodityBoughtForCloudEntity(
            long providerOid, long buyerOid) {
        MarketTier marketTier = cloudTc.getMarketTier(providerOid);
        int providerEntityType = marketTier.getTier().getEntityType();
        if (TierExcluder.EXCLUSION_SUPPORTED_TIER_VALUES.contains(providerEntityType)) {
            return createTierExclusionCommodityBought(buyerOid);
        }
        return Collections.emptySet();
    }

    /**
     * Creates template exclusion commodities bought for cloud consumer that is
     * currently on-prem.
     *
     * @param providerOid oid of the provider
     * @param buyer the cloud consumer
     * @return the list of template exclusion commodities the consumer needs to buy
     */
    @Nonnull
    private Set<CommodityBoughtTO> createTierExclusionCommodityBoughtForMigratingEntity(
        long providerOid, TopologyEntityDTO buyer) {

        TopologyEntityDTO provider = entityOidToDto.get(providerOid);
        if (provider == null) {
            return Collections.emptySet();
        }
        if (provider.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
            return createTierExclusionCommodityBought(buyer.getOid());
        } else if (provider.getEntityType() == EntityType.STORAGE_VALUE) {
            // For on-prem to cloud migration, in source topology, VM's storage side provider is Storage instead of VirtualVolume.
            // However, StorageTier exclusion policy in "Virtual Volume Defaults" is configured as policies on all cloud volume
            // entities. Thus StorageTier exclusion should be retrieved from the volumes.
            Set<Long> volumes = getRelatedVolumesOnProvider(buyer, providerOid);
            return volumes.stream().flatMap(
                            volumeId -> createTierExclusionCommodityBought(volumeId).stream())
                            .collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }

    /**
     * Creates template exclusion commodities which need to be bought to exclude tiers.
     *
     * @param buyerOid the consumer
     * @return the list of template exclusion commodities the consumer needs to buy
     */
    @Nonnull
    private Set<CommodityBoughtTO> createTierExclusionCommodityBought(final long buyerOid) {
        return tierExcluder.getTierExclusionCommoditiesToBuy(buyerOid).stream()
            .map(ct -> {
                final CommoditySpecificationTO commoditySpec =
                    commodityConverter.commoditySpecification(ct);
                return CommodityBoughtTO.newBuilder()
                    .setSpecification(commoditySpec)
                    .build();
            })
            .collect(Collectors.toSet());
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
        final MarketTier marketTier = cloudTc.getMarketTier(providerOid);
        final Set<String> bcKeys = new HashSet<>();
        if (marketTier != null) {
            int providerEntityType = marketTier.getTier().getEntityType();
            TopologyEntityDTO cloudBuyer = entityOidToDto.get(buyerOid);
            if (providerEntityType == EntityType.COMPUTE_TIER_VALUE) {
                Set<Long> connectedStorageMarketTierOids =
                    cloudTc.getMarketTierProviderOidOfType(cloudBuyer, EntityType.STORAGE_TIER_VALUE);
                connectedStorageMarketTierOids.stream().filter(Objects::nonNull)
                    .map(stOid -> dsBasedBicliquer.getBcKey(
                        String.valueOf(providerOid), String.valueOf(stOid)))
                    .filter(Objects::nonNull)
                    .forEach(bcKeys::add);
            }
        }
        return bcKeys.stream()
            .filter(Objects::nonNull)
            .map(this::bcCommodityBought)
            .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * The total used value of storage amount commodities bought by a buyer.
     *
     * @param buyerSL the {@link CommoditiesBoughtFromProvider} of the provider (presumably a VM)
     * @return total used storage amount bought
     */
    private double totalStorageAmountBought(@Nonnull final CommoditiesBoughtFromProvider buyerSL) {
        return buyerSL.getCommodityBoughtList().stream().mapToDouble(commBought ->
                commBought.getCommodityType().getType() == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE
                    ? commBought.getUsed() : 0).sum();
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
                // The entity may be covered by multiple RIs - the first RI is picked as the
                // provider
                long riId = coverage.get().getCouponsCoveredByRiMap().keySet().iterator().next();
                final ReservedInstanceData riData = cloudTc.getRiDataById(riId);
                if (riData != null) {
                    providerId = cloudTc.getRIDiscountedMarketTierIDFromRIData(riData);
                    resultType = RiDiscountedMarketTier.class;
                } else {
                    logger.error("RI: {} for topology entity: {} not found in scope.",
                            riId, topologyEntity.getOid() + "::" + topologyEntity.getDisplayName());
                    providerId = cloudTc.getTraderTOOid(new OnDemandMarketTier(
                            providerTopologyEntity));
                    resultType = OnDemandMarketTier.class;
                }
            } else {
                providerId = cloudTc.getTraderTOOid(new OnDemandMarketTier(
                        providerTopologyEntity));
                resultType = OnDemandMarketTier.class;
            }
        }
        return new Pair<>(providerId, resultType);
    }

    private List<CommodityDTOs.CommodityBoughtTO> convertCommodityBought(
            final TopologyEntityDTO buyer, @Nonnull final CommodityBoughtDTO topologyCommBought,
            @Nullable final Long providerOid,
            final boolean shopTogether,
            final Map<Long, Long> providers,
            final Optional<ScalingGroupUsage> scalingGroupUsage) {
        CommodityType type = topologyCommBought.getCommodityType();
        if (CommodityConverter.isBicliqueCommodity(type)) {
            if (shopTogether || cloudTc.isMarketTier(providerOid)) {
                // skip DSPMAcess and Datastore commodities in the case of shop-together
                // or cloud provider
                return null;
            }
            // convert them to biclique commodities if not shop-together
            CommodityBoughtTO bc = generateBcCommodityBoughtTO(
                    providers.getOrDefault(providerOid, providerOid), type);
            return bc != null ? ImmutableList.of(bc) : null;
        }
        // all other commodities - convert to DTO regardless of shop-together
        return createAndValidateCommBoughtTO(buyer, topologyCommBought, providerOid,
                scalingGroupUsage);
    }

    @VisibleForTesting
    List<CommodityDTOs.CommodityBoughtTO> createAndValidateCommBoughtTO(
            final TopologyEntityDTO buyer,
            CommodityBoughtDTO topologyCommBought, @Nullable final Long providerOid,
            final Optional<ScalingGroupUsage> scalingGroupUsage) {
        List<UsedAndPeak> quantityList = getCommBoughtQuantities(buyer, topologyCommBought,
                providerOid, scalingGroupUsage);
        int slots =
                (topologyCommBought.hasHistoricalUsed() &&
                        topologyCommBought.getHistoricalUsed().getTimeSlotCount() > 0) ?
                        topologyCommBought.getHistoricalUsed().getTimeSlotCount() : 1;
        final Collection<CommoditySpecificationTO> commoditySpecs = commodityConverter.commoditySpecification(
                topologyCommBought.getCommodityType(), slots);

        List<CommodityDTOs.CommodityBoughtTO> boughtTOs = new ArrayList<>();
        int index = 0;
        for (CommoditySpecificationTO spec: commoditySpecs) {
            UsedAndPeak quantities = quantityList.get(index++);
            float quantity = quantities.used;
            float peakQuantity = quantities.peak;
            CommodityBoughtTO.Builder builder = CommodityDTOs.CommodityBoughtTO.newBuilder().setSpecification(spec);
            final int commodityType = topologyCommBought.getCommodityType().getType();
            // Add old capacity for cloud volume or on prem new model volume shoppingList.
            int buyerEntityType = buyer.getEntityType();
            if (OLD_CAPACITY_REQUIRED_ENTITIES_TO_COMMODITIES.containsKey(buyerEntityType)) {
                Set<Integer> oldCapacityRequiredCommodityTypes =
                        OLD_CAPACITY_REQUIRED_ENTITIES_TO_COMMODITIES.get(buyerEntityType);
                if (oldCapacityRequiredCommodityTypes.contains(commodityType)) {
                    CommoditySoldDTO volumeSold = buyer.getCommoditySoldListList()
                            .stream()
                            .filter(sold -> sold.hasCommodityType() &&
                                    sold.getCommodityType().getType() == commodityType)
                            .findFirst()
                            .orElse(null);
                    float oldCapacity = (volumeSold == null) ? 0f : (float)volumeSold.getCapacity();
                    // Convert cloud volume StorageAmount quantity from MB to GB
                    float factor = calculateFactorForCommodityValues(commodityType, buyer);
                    quantity /= factor;
                    peakQuantity /= factor;
                    oldCapacity /= factor;
                    if (oldCapacity != 0) {
                        builder.setAssignedCapacityForBuyer(oldCapacity);
                    }
                }
            } else if (isCloudMigration && CLOUD_VOLUME_COMMODITIES_UNIT_CONVERSION.contains(commodityType)) {
                // Storage amount used is coming in MB, market expects in GB which is tier capacity.
                // Issue seen for cloud migration case where source entities with storage in MB,
                // are being migrated to cloud storage tiers like GP2.
                // As this is market specific, doing this here rather than in topology, so that it
                // doesn't affect projected entity or API calculations, which might still be
                // expecting quantity in MB and might perform their own unit conversion.
                quantity /= Units.KIBI;
                peakQuantity /= Units.KIBI;
            }
            // Cloud migration lift and shift plans only considers storage amount in volume actions.
            // M2 assigns storage access value based on the storage amount. Set storage access value
            // to 0 in shopping list.
            if (isCloudMigration && !isCloudResizeEnabled
                    && (buyerEntityType == EntityType.VIRTUAL_VOLUME_VALUE
                    || buyerEntityType == EntityType.VIRTUAL_MACHINE_VALUE)
                    && commodityType == CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE) {
                quantity = 0;
                peakQuantity = 0;
            }
            builder.setQuantity(quantity).setPeakQuantity(peakQuantity);
            populateHistoricalQuantity(commodityType, buyer, builder);
            boughtTOs.add(builder.build());
        }
        logger.debug("Created {} bought commodity TOs for {}",
            boughtTOs::size, () -> topologyCommBought);
        return boughtTOs;
    }

    private static void populateHistoricalQuantity(
            final int commodityType,
            @Nonnull final TopologyEntityDTO buyer,
            @Nonnull final CommodityBoughtTO.Builder builder) {
        if (buyer.hasTypeSpecificInfo()) {
            final TypeSpecificInfo typeSpecificInfo = buyer.getTypeSpecificInfo();
            if (commodityType == CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE
                    && typeSpecificInfo.hasVirtualVolume()
                    && typeSpecificInfo.getVirtualVolume().hasHourlyBilledOps()) {
                builder.setHistoricalQuantity(
                        (float)typeSpecificInfo.getVirtualVolume().getHourlyBilledOps());
            }
        }
    }

    /**
     * Return the used and peak used amounts for the given commodity bought.  If the amounts were
     * previously calculated as part of the top commodity usage computation phase, those values
     * will be used instead.
     * @param buyer buyer
     * @param topologyCommBought commodity to get usage for
     * @param providerOid OID of provider
     * @param scalingGroupUsage pre-calculated scaling group usage, if available.
     * @return a list of a pairs, each pair with the used and peak values.
     */
    private List<UsedAndPeak> getCommBoughtQuantities(
            final TopologyEntityDTO buyer,
            final CommodityBoughtDTO topologyCommBought,
            @Nullable final Long providerOid,
            final Optional<ScalingGroupUsage> scalingGroupUsage) {
        if (scalingGroupUsage.isPresent()) {
            Optional<Double> cachedUsage = scalingGroupUsage.get()
                    .getUsageForCommodity(topologyCommBought);
            if (cachedUsage.isPresent()) {
                final Float usage = cachedUsage.get().floatValue();
                return Collections.singletonList(new UsedAndPeak(usage, usage));
            }
        }
        return getCommBoughtQuantities(buyer, topologyCommBought, providerOid);
    }

    private List<UsedAndPeak> getCommBoughtQuantities(final TopologyEntityDTO buyer,
                                                             CommodityBoughtDTO topologyCommBought,
                                                             @Nullable final Long providerOid) {

        //If commBought is in this map, that means its reservation is greater than used, and we use reservation as quantity
        float[] usedQuantities = new float[1];
        float[] peakQuantities = new float[1];
        if (commoditiesWithReservationGreaterThanUsed.containsKey(buyer.getOid())
            && commoditiesWithReservationGreaterThanUsed.get(buyer.getOid()).containsKey(topologyCommBought)) {
            logger.debug("replacing buyer {}'s {} comm used with reservation", buyer.getOid(), topologyCommBought.getCommodityType().getType());
            usedQuantities[0] = (float)topologyCommBought.getReservedCapacity();
            peakQuantities[0] = (float)topologyCommBought.getPeak();
        } else {
            final float[][] newQuantity = getResizedCapacity(buyer, topologyCommBought, providerOid);
            usedQuantities = newQuantity[0];
            peakQuantities = newQuantity[1];
            if (newQuantity.length == 0) {
                logger.warn("Received  empty resized quantities for {}", topologyCommBought);
                return new ArrayList<>();
            }
        }
        List<UsedAndPeak> newQuantityList = new ArrayList<>();
        int maxLength = Math.max(usedQuantities.length, peakQuantities.length);
        for (int index = 0; index < maxLength; index++) {
            float usedQuantity = getQuantity(index, usedQuantities, topologyCommBought, "used");
            // Bought Flow-0 commodity must have quantity of 1.
            if (topologyCommBought.getCommodityType().getType() ==
                    CommodityDTO.CommodityType.FLOW_VALUE &&
                    "FLOW-0".equals(topologyCommBought.getCommodityType().getKey())) {
                usedQuantity = 1;
            }
            usedQuantity *= topologyCommBought.getScalingFactor();

            float peakQuantity = getQuantity(index, peakQuantities, topologyCommBought, "peak");
            peakQuantity *= topologyCommBought.getScalingFactor();
            newQuantityList.add(new UsedAndPeak(usedQuantity, peakQuantity));
        }
        return newQuantityList;
    }

    private float getQuantity(final int index, final float[] quantities,
                              final CommodityBoughtDTO topologyCommBought, String quantityType) {
        int size = quantities.length;
        float quantity = (index < size) ? quantities[index] : 0;
        if (quantity < 0) {
            // We don't want to log every time we get used = -1 because mediation
            // sets some values to -1 as default.
            if (logger.isDebugEnabled() || quantity != -1) {
                logger.info("Setting negative" + quantityType + " value for "
                        + topologyCommBought.getCommodityType() + " to 0.");
            }
            quantity = 0;
        }
        return quantity;
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
                .setSpecification(commodityConverter.commoditySpecificationBiClique(key))
                .build());
    }

    /**
     * Convert a single {@link CommoditySoldTO} market dto to the corresponding
     * {@link CommoditySoldDTO} XL model object.
     * <p/>
     * Note that if the original CommoditySoldDTO has a 'scaleFactor', then
     * we reverse the scaling that had been done on the way in to the market.
     *
     * @param traderTO The TraderTO of the trader selling the commodity.
     * @param commSoldTO the market CommdditySoldTO to convert
     * @param timeSlotsByCommType Timeslot values arranged by {@link CommodityBoughtTO}
     * @param traderOidToEntityDTO Map from traderTO OID to original TopologyEntityDTO.
     * @return a {@link CommoditySoldDTO} equivalent to the original.
     */
    @Nonnull
    private Optional<TopologyDTO.CommoditySoldDTO> commSoldTOtoCommSoldDTO(
            final TraderTO traderTO,
            @Nonnull final CommoditySoldTO commSoldTO,
            @Nonnull Map<CommodityType, List<Double>> timeSlotsByCommType,
            @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> traderOidToEntityDTO) {

        final long traderOid = traderTO.getOid();
        float peak = commSoldTO.getPeakQuantity();
        if (peak < 0) {
            conversionErrorCounts.recordError(ErrorCategory.PEAK_NEGATIVE,
                    commSoldTO.getSpecification().getDebugInfoNeverUseInCode());
            logger.trace("The entity with negative peak is {}", traderOid);
            peak = 0;
        }

        Optional<CommodityType> commTypeOptional =
                commodityConverter.marketToTopologyCommodity(commSoldTO.getSpecification());
        if (!commTypeOptional.isPresent() ||
            tierExcluder.isCommSpecTypeForTierExclusion(commSoldTO.getSpecification())) {
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
            getCommodityIndex().getCommSold(traderOid, commType);
        if (!originalCommoditySold.isPresent() && projectedTraderTO.getCloneOf() != 0) {
            originalCommoditySold = getCommodityIndex().getCommSold(projectedTraderTO.getCloneOf(), commType);
        }

        float reverseScaleQuantity = commSoldTO.getQuantity();
        float reverseScaleCapacity = commSoldTO.getCapacity();
        float reverseScalePeakQuantity = peak;
        double scalingFactor = 1.0;
        if (originalCommoditySold.isPresent()) {
            scalingFactor = originalCommoditySold.get().getScalingFactor();
            if (scalingFactor > EPSILON) {
                double inverseScalingFactor = 1.0 / scalingFactor;
                reverseScaleQuantity *= inverseScalingFactor;
                reverseScaleCapacity *= inverseScalingFactor;
                reverseScalePeakQuantity *= inverseScalingFactor;
            }
        }
        if (useVMReservationAsUsed) {
            reverseScaleQuantity -= projectedProviderUsedSubtractionMap.getOrDefault(traderOid, Collections.emptyMap())
                .getOrDefault(commType, 0d).floatValue();
            if (reverseScaleQuantity < 0) {
                logger.error("reverseScaleQuantity {} is negative. Entity oid {}. Commodity Sold {}",
                    reverseScaleQuantity, traderOid, commType.getType());
                reverseScaleQuantity = 0;
            }
        }

        double capacity;
        // Keep original commodity capacity when given trader is not allowed to resize/scale to avoid
        // inconsistent current and projected capacity values.
        if (shouldKeepOrigCapacity(traderTO, traderOidToEntityDTO)) {
            capacity = reverseScaleCapacity;
        } else {
            capacity = updateCommoditySoldCapacity(traderTO, commType, marketTier,
                reverseScaleCapacity, commSoldTO.getSpecification().getBaseType(),
                scalingFactor);
        }
        // Do a fuzzy comparison of the capacity with the original capacity. If they are close
        // enough, just use the original capacity, as there may be precision loss during CPU
        // scale and reverse scale even though the capacity is not changed.
        // For example:
        //   - The original capacity (double): 1E12
        //   - The scalingFactor (double): 3.186912
        //   - The scaled capacity (float): 3.18691187E12 (from double to float with precision loss)
        //   - The reverse scaled capacity (float): 9.9999993E11
        if (originalCommoditySold.isPresent()) {
            final double originalCapacity = originalCommoditySold.get().getCapacity();
            if (DoubleMath.fuzzyEquals(1.0D, capacity/originalCapacity, EPSILON)) {
                capacity = originalCapacity;
            }
        }
        CommoditySoldDTO.Builder commoditySoldBuilder = CommoditySoldDTO.newBuilder()
            .setCapacity(capacity)
            .setUsed(reverseScaleQuantity)
            .setPeak(reverseScalePeakQuantity)
            .setIsResizeable(commSoldTO.getSettings().getResizable())
            .setEffectiveCapacityPercentage(
                commSoldTO.getSettings().getUtilizationUpperBound() * 100)
            .setCommodityType(commType)
            .setIsThin(commSoldTO.getThin())
            .setCapacityIncrement((float)reverseScaleComm(
                    commSoldTO.getSettings().getCapacityIncrement(),
                    originalCommoditySold, CommoditySoldDTO::getScalingFactor));

        // set hot add / hot remove, if present
        originalCommoditySold
            .filter(CommoditySoldDTO::hasHotResizeInfo)
            .map(CommoditySoldDTO::getHotResizeInfo)
            .ifPresent(commoditySoldBuilder::setHotResizeInfo);

        if (originalCommoditySold.isPresent()) {
            commoditySoldBuilder.setScalingFactor(originalCommoditySold.get().getScalingFactor());
            if (originalCommoditySold.get().hasHistoricalUsed()) {
                if (originalCommoditySold.get().getHistoricalUsed().hasPercentile()) {
                    // populate the projected percentile.
                    float existingPercentile = (float)originalCommoditySold.get().getHistoricalUsed()
                            .getPercentile();
                    double existingCapacity = originalCommoditySold.get().getCapacity();
                    float projectedPercentile = (float)(existingCapacity / capacity) * existingPercentile;
                    commoditySoldBuilder.setHistoricalUsed(HistoricalValues.newBuilder()
                            .setPercentile(projectedPercentile)
                            .build());
                } else if (originalCommoditySold.get().getHistoricalUsed().hasMovingMeanPlusStandardDeviations()) {
                    // populate the projected moving average. This is same as the re-scaled projected quantity.
                    commoditySoldBuilder.setHistoricalUsed(HistoricalValues.newBuilder()
                            .setMovingMeanPlusStandardDeviations(reverseScaleQuantity).build());
                }
            }
        }

        // Set timeslot values if applies
        if (timeSlotsByCommType.containsKey(commType)) {
            if (commoditySoldBuilder.hasHistoricalUsed()) {
                commoditySoldBuilder.setHistoricalUsed(
                    commoditySoldBuilder.getHistoricalUsed().toBuilder()
                        .addAllTimeSlot(timeSlotsByCommType.get(commType))
                        .build());
            } else {
                commoditySoldBuilder.setHistoricalUsed(HistoricalValues.newBuilder()
                    .addAllTimeSlot(timeSlotsByCommType.get(commType))
                    .build());
            }
        }

        return Optional.of(commoditySoldBuilder.build());
    }

    /**
     * Check if commodity of given traderTO should keep original capacity without being needed to
     * update. For example, keep original commodity capacity when original TopologyEntityDTO of a given
     * traderTO is the provider of containers or container pods because such entities are not allowed
     * to resize/scale.
     *
     * @param traderTO   Given {@link TraderTO}.
     * @param traderOidToEntityDTO Map from traderTO OID to original TopologyEntityDTO.
     * @return True if original commodity capacity should be kept for the given traderTO.
     */
    private boolean shouldKeepOrigCapacity(@Nonnull final TraderTO traderTO,
                                           @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> traderOidToEntityDTO) {
        TopologyEntityDTO originalEntity = traderOidToEntityDTO.get(traderTO.getOid());
        if (originalEntity == null) {
            // Get original entity from original traderTO if this is a cloned trader.
            originalEntity = traderOidToEntityDTO.get(traderTO.getCloneOf());
        }
        return originalEntity != null && providersOfContainers.contains(originalEntity.getOid());
    }

    /**
     * This method allows us to use assignedCapacityForBuyer if available. Right now this is only allowed for
     * entities and commodities combos in {@link #OLD_CAPACITY_REQUIRED_ENTITIES_TO_COMMODITIES}.
     *
     * @param traderTO           {@link TraderTO} whose commoditiesSold are to be converted into DTOs.
     * @param commType           commodity type.
     * @param marketTier         the primary market tier.
     * @param reverseScaledCapacity commodity sold capacity that has been reverse-scaled by scaling factor.
     * @param commSoldToBaseType The commodity base type for the commoditySoldTO whose capacity is being adjusted.
     * @param scalingFactor Given scaling factor to apply to VCPU commodity that should scale.
     * @return double capacity value.
     */
    private double updateCommoditySoldCapacity(final @Nonnull TraderTO traderTO,
                                               final @Nonnull CommodityType commType,
                                               final @Nonnull MarketTier marketTier,
                                               final float reverseScaledCapacity,
                                               final int commSoldToBaseType,
                                               final double scalingFactor) {
        // Filter based on if the current CommodityType and entityType matches.
        final int entityType = traderTO.getType();
        if (OLD_CAPACITY_REQUIRED_ENTITIES_TO_COMMODITIES.containsKey(entityType) &&
                OLD_CAPACITY_REQUIRED_ENTITIES_TO_COMMODITIES.get(traderTO.getType()).contains(commType.getType())) {

            // Sorted list of commodityBoughtTOs with matching commType.
            List<CommodityBoughtTO> commodityBoughtTOs =
                    traderTO.getShoppingListsList()
                            .stream().map(shoppingListTO -> {
                        Optional<CommodityBoughtTO> commBoughtOpt = shoppingListTO.getCommoditiesBoughtList().stream()
                                .filter(commBought -> commBought.getSpecification().hasBaseType()
                                        && commBought.getSpecification().getBaseType() == commType.getType()
                                        && commBought.hasAssignedCapacityForBuyer()).findFirst();
                        return commBoughtOpt.orElse(null);
                    }).filter(Objects::nonNull)
                            .sorted((o1, o2) -> Float.compare(o1.getAssignedCapacityForBuyer(), o2.getAssignedCapacityForBuyer()))
                            .collect(Collectors.toList());

            // If the list of commodityBoughtTOs is not empty, Return the smallest assignedCapacityForBuyer as capacity.
            if (!commodityBoughtTOs.isEmpty()) {
                CommodityBoughtTO commodityBoughtTO = commodityBoughtTOs.iterator().next();
                return commodityBoughtTO.getAssignedCapacityForBuyer() *
                        calculateFactorForCommodityValues(commType.getType(), entityType);
            }
        }
        // If assignedCapacityForBuyer is not available, get capacity from commSoldTO.
        return getCapacityForCommodity(reverseScaledCapacity, commSoldToBaseType,
            marketTier, commType, traderTO.getOid(), scalingFactor);
    }

    /**
     * Find the capacity of the old provider for a given sold commodity TO.
     * @param reverseScaledCapacity commodity sold capacity that has been reverse-scaled by scaling factor.
     * @param commSoldToBaseType The commodity base type for the commoditySoldTO whose capacity is being adjusted.
     * @param marketTier the current market tier for the trader selling the commodity.
     * @param commType the commodity type.
     * @param traderOid the trader OID.
     * @param scalingFactor Given scaling factor to apply to VCPU commodity that should scale.
     * @return the new capacity for the passed sold TO.
     */
    private float getCapacityForCommodity(final float reverseScaledCapacity,
                                          final int commSoldToBaseType,
                                          final MarketTier marketTier,
                                          final CommodityType commType,
                                          final long traderOid,
                                          final double scalingFactor) {
        float capacity = reverseScaledCapacity;

        // If it is a tier based cloud sold TO, return the new provider capacity
        if (marketTier != null && marketTier.getTier() != null) {
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
            Optional<CommoditySoldDTO> tierSoldDTO =
                    marketTier.getTier().getCommoditySoldListList().stream().filter(
                            commoditySoldDTO -> commoditySoldDTO.getCommodityType().getType() == tierComodityType
                                    && compareKeys(commoditySoldDTO.getCommodityType(), commType)).findFirst();
            if (tierSoldDTO.isPresent() && tierSoldDTO.get().hasCapacity()) {
                return (float)tierSoldDTO.get().getCapacity();
            } else {
                logger.warn("Could not determine new provider capacity for sold commodity" +
                        " of type {} for trader {}.Using usage as received from trader from market.",
                        commType.getType(), traderOid);
            }
        } else if (commSoldToBaseType == CommodityDTO.CommodityType.VCPU_VALUE) {
            capacity = calculateVCPUResizeCapacityForVM(traderOid, reverseScaledCapacity, scalingFactor);
        }
        return capacity;
    }

    /**
     * Compare 2 commodity keys.
     * If both keys are missed we consider that as match.
     * If one of the keys are missed we consider that as mismatch.
     * If both keys are present - we compare keys.
     *
     * @param dest destination commodity key
     * @param orig sold commodity key
     * @return true is keys match
     */
    @VisibleForTesting
    static boolean compareKeys(CommodityType dest, CommodityType orig) {
        return (!dest.hasKey() && !orig.hasKey()) || (dest.hasKey() && orig.hasKey()
                && dest.getKey().equals(orig.getKey()));
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
     * @param reverseScaledCapacity commodity sold capacity that has been reverse-scaled by scaling factor.
     * @param scalingFactor Given scaling factor to apply to VCPU commodity that should scale.
     * @return the correct VCPU capacity
     */
    private float calculateVCPUResizeCapacityForVM(final long traderOid,
                                                   final float reverseScaledCapacity,
                                                   final double scalingFactor) {
        float capacity = reverseScaledCapacity;
        if (oidToProjectedTraderTOMap.get(traderOid).getType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            return capacity;
        }
        // A VM may be cloned from another VM.
        long originalTraderOid = oidToProjectedTraderTOMap.get(traderOid).hasCloneOf() ?
            oidToProjectedTraderTOMap.get(traderOid).getCloneOf() : traderOid;
        final float originalVcpuCapacity = oidToOriginalTraderTOMap.get(originalTraderOid).getVcpuCapacity();
        if (originalVcpuCapacity < 0) {
            return capacity;
        }
        // Check if VCPU is resized.
        // originalVcpuCapacity has CPU scalingFactor multiplied. So to check if VCPU is resized, we
        // need to reverse the scalingFactor multiplication of originalVcpuCapacity first and then
        // compare with the given reversed scaled capacity.
        float reverseOriginalCommSoldCapacity = (float)(originalVcpuCapacity / scalingFactor);
        boolean isVCPUresized = Math.abs(capacity - reverseOriginalCommSoldCapacity) > 0.001f;
        if (!isVCPUresized) {
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
            logger.error("VM with OID = {} has no PM provider.", traderOid);
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
    public static <T> double reverseScaleComm(final double valueToReverseScale,
            @Nonnull final Optional<T> commodity,
            @Nonnull Function<T, Double> scalingFactorExtractor) {
        return commodity.map(scalingFactorExtractor).filter(sf -> {
            // Scaling factor should be positive, and not an infinitely small value.
            return (sf - EPSILON) > 0;
        }).map(sf -> valueToReverseScale / sf).orElse(valueToReverseScale);
    }

    public CommodityConverter getCommodityConverter() {
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
            // TODO: This logic is incorrect. We are putting in ShoppingListInfos saying that the sellerId
            // is null and the sellerEntityType is null when it is not true. This causes problems downstream
            // when we attempt to look up the information from this map.
                    new ShoppingListInfo(l.getNewShoppingList(), l.getBuyer(), null,
                                    Collections.emptySet(), null, null, Lists.newArrayList())));
    }

    /**
     * Given TraderTOs to be sent to market remove skipped entities from this set.
     * @param traderTOs set of TraderTOs sent to market.
     */
    public void removeSkippedEntitiesFromTraderTOs(Long2ObjectMap<TraderTO> traderTOs) {
        // Remove all skipped traders as we don't want to send them to market
        // and only needed them for scoping.
        for (long skippedEntityOid : skippedEntities.keySet()) {
            TraderTO traderTO = traderTOs.remove(skippedEntityOid);
            if (traderTO != null) {
                final Set<Long> shoppingListsOids = traderTO.getShoppingListsList().stream().map(ShoppingListTO::getOid)
                        .collect(Collectors.toSet());
                logger.debug("Remove following ShoppingListTOs {} from cloudVmComputeShoppingListIDs, because traderTO {} is skipped",
                        () -> shoppingListsOids.stream().map(String::valueOf).collect(Collectors.joining(",")),
                    traderTO::getDebugInfoNeverUseInCode);
                cloudVmComputeShoppingListIDs.removeAll(shoppingListsOids);

            }
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

    /**
     * Filter the license commodity bought for a VM if its an Unknown License. If a VM has an unknown
     * license, it should not be going into the market shopping for a license access commodity. It
     * should not shop for one and we should scope it to the cost tuple having no value for license
     * when getting a quote from a TP.
     *
     * @param commodityBoughtDTOList List of CommodityBoughtDTO's.
     * @param entityDTO The entity DTO.
     *
     * @return Stream with the filtered license commodities
     */
    public Stream<CommodityBoughtDTO> filterUnknownLicense(List<CommodityBoughtDTO> commodityBoughtDTOList, TopologyEntityDTO entityDTO) {
        if (EnvironmentType.CLOUD == entityDTO.getEnvironmentType() && entityDTO.getTypeSpecificInfo().hasVirtualMachine()) {
            if (commodityBoughtDTOList.stream()
                    .filter(s -> s.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                    .anyMatch(s -> OSType.UNKNOWN_OS.name().equals(s.getCommodityType().getKey()))) {
                return commodityBoughtDTOList.stream().filter(s -> s.getCommodityType().getType() != CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE);
            }
        }
        return commodityBoughtDTOList.stream();
    }

    /**
     * Create the commodity index from the input TopologyDTO's.
     * <p/>
     * Note: It is illegal to call this before converting entities to market because
     * the OID to DTO map required to populate the commodity index properly will
     * be empty.
     *
     * @param commodityIndexFactory Factory to use to create the commodity index.
     * @return the commodity index created from the input TopologyDTO's.
     */
    private CommodityIndex createCommodityIndex(final CommodityIndexFactory commodityIndexFactory) {
        if (!convertToMarketComplete) {
            throw new IllegalStateException("Illegal request to createCommodityIndex before convertToMarket is complete. "
                + "If this is a unit test, you can call #convertToMarket with an empty map before calling #convertFromMarket. "
                + "Seeing this exception in production is a serious error.");
        }

        final CommodityIndex index = commodityIndexFactory.newIndex();
        for (TopologyDTO.TopologyEntityDTO dto : entityOidToDto.values()) {
            index.addEntity(dto);
        }
        return index;
    }

    /**
     * Create providerUsedModificationMap.
     * provider oid -> commodity type -> used value of all consumers to be removed of this provider.
     * This map is used to update the utilization of providers if there are entities to be removed.
     * This can only happen in a plan with entities to remove.
     *
     * @param entityOidToDto topology entity DTOs
     * @param oidsToRemove oids to remove
     * @return providerUsedSubtractionMap
     */
    @VisibleForTesting
    Map<Long, Map<TopologyDTO.CommodityType, UsedAndPeak>> createProviderUsedModificationMap(
            final Map<Long, TopologyEntityDTO> entityOidToDto, final Set<Long> oidsToRemove) {
        if (oidsToRemove.isEmpty()) {
            return Maps.newHashMap();
        }

        final Map<Long, Map<TopologyDTO.CommodityType, UsedAndPeak>> providerUsedSubtractionMap = new HashMap<>();
        for (long oid : oidsToRemove) {
            final TopologyEntityDTO entity = entityOidToDto.get(oid);
            for (CommoditiesBoughtFromProvider commBoughtProvider : entity.getCommoditiesBoughtFromProvidersList()) {
                final TopologyEntityDTO provider = entityOidToDto.get(commBoughtProvider.getProviderId());
                // SKip non-workload type entity and entity without provider.
                if (!WORKLOAD_ENTITY_TYPES.contains(entity.getEntityType()) || provider == null) {
                    continue;
                }

                for (CommodityBoughtDTO commBought : commBoughtProvider.getCommodityBoughtList()) {
                    // Skip segmentation commodity.
                    if (commBought.getCommodityType().hasKey()) {
                        continue;
                    }
                    final Map<TopologyDTO.CommodityType, UsedAndPeak> commodityUsed =
                        providerUsedSubtractionMap.computeIfAbsent(commBoughtProvider.getProviderId(),
                            key -> new HashMap<>());
                    final List<UsedAndPeak> quantityList =
                        getCommBoughtQuantities(entity, commBought, provider.getOid());
                    // The size of quantityList is greater than 1 only when the commBought is a time slot commodity.
                    if (quantityList.size() >= 1) {
                        UsedAndPeak currentVal = commodityUsed.containsKey(commBought.getCommodityType())
                            ? commodityUsed.get(commBought.getCommodityType()) : new UsedAndPeak(0.0f, 0.0f);
                        commodityUsed.put(commBought.getCommodityType(), new UsedAndPeak(
                            currentVal.used + quantityList.get(0).used, currentVal.peak + quantityList.get(0).peak));
                    }
                }
            }
        }

        return providerUsedSubtractionMap;
    }

    /**
     * This method add commBought.used - commBought.reservation into the providerUsedSubtractionMap.
     * So we add up the difference onto the provider's sold commodity later.
     * @param providerUsedSubtractionMap The subtraction map to be populated
     * @param commBought The commBought whose used - reservation needs to be saved.
     * @param providerId The provider that provides the commodity.
     */
    private void addReservationUsedDiffIntoProviderUsedModificationMap(
           Map<Long, Map<TopologyDTO.CommodityType, UsedAndPeak>> providerUsedSubtractionMap,
           CommodityBoughtDTO commBought, long providerId) {
       Map<TopologyDTO.CommodityType, UsedAndPeak> commodityUsed = providerUsedSubtractionMap
               .computeIfAbsent(providerId, k -> new HashMap());
       //We need to add reservation - used onto the provider's sold used,
       //so add used - reservation into this modification map, as it is used for subtraction.
       UsedAndPeak currentVal = commodityUsed.containsKey(commBought.getCommodityType())
               ? commodityUsed.get(commBought.getCommodityType()) : new UsedAndPeak(0.0f, 0.0f);
       float diff = (float)((commBought.getUsed() - commBought.getReservedCapacity()) * commBought.getScalingFactor());
       logger.debug("Adding modified value {} into providermodificationmap for provider {}, commType {}",
               diff, providerId, commBought.getCommodityType().getType());
       commodityUsed.put(commBought.getCommodityType(), new UsedAndPeak(
               currentVal.used + diff, currentVal.peak));
   }

    /**
     * Sets the cost notification status to be used by topology converter for setting movable on cloud entities.
     * If the cloud cost notification fails, we will set movable false on cloud entities.
     *
     * @param listener The listened for the notification.
     * @param analysis The analysis currently running.
     *
     * @throws InterruptedException An interrupted exception.
     */
    public void setCostNotificationStatus(AnalysisRICoverageListener listener, Analysis analysis)
            throws InterruptedException {
        final long waitStartTime = System.currentTimeMillis();
        try {
            CostNotification notification = listener.receiveCostNotification(analysis).get();
            final StatusUpdate statusUpdate = notification.getStatusUpdate();
            final Status status = statusUpdate.getStatus();
            if (status != Status.SUCCESS) {
                logger.error("WARNING!!:Cost notification reception failed for analysis with context id"
                                + " : {}, topology id: {} with status: {} and message: {}. This will result"
                                + " in movable being set to false for cloud entities.", analysis.getContextId(), analysis.getTopologyId(), status,
                        statusUpdate.getStatusDescription());
                costNotificationStatus = Status.FAIL;
            } else {
                logger.debug("Cost notification with a success status received for analysis "
                        + "with context id: {}, topology id: {}", analysis.getContextId(), analysis.getTopologyId());
                costNotificationStatus = Status.SUCCESS;
            }
        } catch (ExecutionException e) {
            logger.error(
                    String.format("Error while receiving cost notification for analysis %s. WARNING!!:"
                                    + " Movable will be set to false for cloud entities. ",
                            analysis.getTopologyInfo()), e);
            costNotificationStatus = Status.FAIL;
        } finally {
            final long waitEndTime = System.currentTimeMillis();
            logger.debug("Analysis with context id: {}, topology id: {} waited {} ms for the "
                            + "cost notification.", analysis.getContextId(), analysis.getTopologyId(),
                    waitEndTime - waitStartTime);
        }
    }

    /**
     * Returns a function that decides if Compliance risk needs to be overridden in
     * ActionInterpreter. For cloud migration, we override Optimized plan actions with VM/Vol moves.
     * For volumes, a change provider explanation is provided in the return based on whether there
     * is a change to a higher tier or not. If no tier such tier change, we check for a disk size
     * increase, if so, risk is marked as Performance. Default risk is Efficiency.
     *
     * @return BiFunction that decides if Compliance risk explanation is to be overridden.
     * For compute tier, we want to override (so flag is true), but we don't actually calculate
     * the override explanation, that is calculated by ActionInterpreter.
     * For storage tier, if it is one of the 'higher' tiers like IO1/IO2/Ultra, then it is a
     * Performance risk. Else, if there is a disk size increase compared to pre-action, then it is
     * Performance, else it is an Efficiency (default option).
     */
    @VisibleForTesting
    BiFunction<MoveTO, Map<Long, ProjectedTopologyEntity>,
            Pair<Boolean, ChangeProviderExplanation>> getExplanationOverride() {
        return (moveTO, projectedTopology) -> {
            if (!isCloudResizeEnabled) {
                return new Pair<>(false, null);
            }
            final MarketTier marketTier = cloudTc.getMarketTier(moveTO.getDestination());
            if (marketTier == null) {
                return new Pair<>(false, null);
            }
            if (marketTier.getTier().getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
                return new Pair<>(true, null);
            }
            if (marketTier.getTier().getEntityType() != EntityType.STORAGE_TIER_VALUE) {
                return new Pair<>(false, null);
            }
            final ShoppingListInfo slInfo = shoppingListOidToInfos.get(moveTO
                    .getShoppingListToMove());
            if (slInfo == null) {
                return new Pair<>(false, null);
            }
            long buyerVmId = slInfo.getBuyerId();
            final String tierName = marketTier.getTier().getDisplayName();
            ChangeProviderExplanation explanation = null;
            // Using IOPS as the reason that we have congestion (performance risk).
            final ReasonCommodity iopsReasonCommodity = ReasonCommodity.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(
                            CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE).build())
                    .build();
            if (StorageTier.IO1.getDisplayName().equals(tierName)
                    || StorageTier.IO2.getDisplayName().equals(tierName)
                    || StorageTier.MANAGED_ULTRA_SSD.getDisplayName().equals(tierName)) {
                // If moving to any of these higher tiers, then it is performance.
                explanation = ChangeProviderExplanation.newBuilder()
                        .setCongestion(ChangeProviderExplanation.Congestion.newBuilder()
                                .addCongestedCommodities(iopsReasonCommodity)).build();
                logger.trace("MCP Performance risk (VM: {}, tier: {}), moveTO: {}.",
                        buyerVmId, tierName, moveTO);
            } else {
                explanation = checkVolumeDiskSizeIncrease(slInfo, buyerVmId, tierName,
                        iopsReasonCommodity, projectedTopology);
            }
            if (explanation == null) {
                // If not performance, it is efficiency
                explanation = ChangeProviderExplanation.newBuilder()
                        .setEfficiency(ChangeProviderExplanation.Efficiency.getDefaultInstance())
                        .build();
                logger.trace("MCP Efficiency risk (VM: {}, tier: {}).", buyerVmId, tierName);
            }
            return new Pair<>(true, explanation);
        };
    }

    /**
     * Checks if there is a volume disk size increase for cloud migration (Optimized plan) actions.
     * If disk size increase, then a Performance explanation is returned.
     *
     * @param slInfo VM shopping list info.
     * @param buyerVmId VM that volume is a part of.
     * @param tierName New storage tier name.
     * @param iopsReasonCommodity Reason (Iops) to use for the Performance risk.
     * @param projectedTopology Projected topology containing the after-action disk size.
     * @return Performance risk change provider explanation if disk increase, or null.
     */
    @Nullable
    private ChangeProviderExplanation checkVolumeDiskSizeIncrease(
            @Nonnull final ShoppingListInfo slInfo, long buyerVmId, @Nonnull final String tierName,
            @Nonnull final ReasonCommodity iopsReasonCommodity,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        // TODO on-prem vvs p2 - support two scenarios of volumes shopping separately or together

        // For onPrem -> cloud, we have volumeId in resourceId field, for cloud -> cloud, it is
        // available in the collapsedBuyerId field instead.
        final Long volumeId = slInfo.getActingId();
        if (volumeId == null) {
            logger.trace("MCP Performance risk (VM: {}, tier: {}) No volume id available.",
                    buyerVmId, tierName);
            return null;
        }
        ProjectedTopologyEntity projectedVolume = projectedTopology.get(volumeId);
        if (projectedVolume == null || projectedVolume.getEntity().getEntityType()
                != EntityType.VIRTUAL_VOLUME_VALUE) {
            logger.trace("MCP Performance risk (VM: {}, tier: {}) No projected volume {}.",
                    buyerVmId, tierName, volumeId);
            return null;
        }
        // Get disk size before plan.
        double diskSizeBeforeMb = slInfo.getCommodities().stream()
                .filter(commBought -> commBought.getCommodityType().getType()
                        == CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE)
                .map(CommodityBoughtDTO::getUsed)
                .findFirst()
                .orElse(0d);

        // Get disk size after from projected volume.
        double diskSizeAfterMb = projectedVolume.getEntity().getCommoditySoldListList().stream()
                .filter(commSold -> commSold.getCommodityType().getType()
                        == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                .map(CommoditySoldDTO::getCapacity)
                .findAny()
                .orElse(0d);

        logger.trace("MCP Performance risk (VM: {}, tier: {}) Vol: {}, disk size {} -> {} MB.",
                buyerVmId, tierName, volumeId, diskSizeBeforeMb, diskSizeAfterMb);
        if (diskSizeAfterMb > diskSizeBeforeMb) {
            return ChangeProviderExplanation.newBuilder()
                    .setCongestion(ChangeProviderExplanation.Congestion.newBuilder()
                            .addCongestedCommodities(iopsReasonCommodity)).build();
        }
        return null;
    }

    /**
     * Set that the convertToMarket call has completed.
     */
    @VisibleForTesting
    void setConvertToMarketComplete() {
        convertToMarketComplete = true;
    }

    /**
     * A small helper class that contains float values for used and peak quantities.
     */
    static class UsedAndPeak {
        /**
         * The used quantity.
         */
        public final float used;
        /**
         * The peak quantity.
         */
        public final float peak;

        /**
         * Create a new {@link UsedAndPeak}.
         *
         * @param used The used value.
         * @param peak The peak value.
         */
        UsedAndPeak(final float used, final float peak) {
            this.used = used;
            this.peak = peak;
        }
    }

    private Set<Long> getRelatedVolumesOnProvider(TopologyEntityDTO vm, long storageProvider) {
        // TODO on-prem vvs p2 we will have on-prem probes optionally discovering
        // - either vm->volume->storage commodities
        // - or vm->storage commodities and multiple associated connected volumes
        // account for both scenarios here
        Set<TopologyEntityDTO> volumes = vm.getConnectedEntityListList().stream()
                        .filter(connectedEntity -> connectedEntity
                                        .getConnectedEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                        .map(ConnectedEntity::getConnectedEntityId).map(entityOidToDto::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
        Set<Long> matchingVolumeIds = new HashSet<>();
        for (TopologyEntityDTO volume : volumes) {
            volume.getConnectedEntityListList().stream()
                            .map(ConnectedEntity::getConnectedEntityId)
                            .filter(storageId -> storageId == storageProvider)
                            .findAny()
                            .ifPresent(storageId -> matchingVolumeIds.add(volume.getOid()));
        }
        return matchingVolumeIds;
    }

    /**
     * Set useVMReservationAsUsed feature flag.
     *
     * @param useVMReservationAsUsed useVMReservationAsUsed
     */
    @VisibleForTesting
    void setUseVMReservationAsUsed(final boolean useVMReservationAsUsed) {
        this.useVMReservationAsUsed = useVMReservationAsUsed;
    }

    /**
     * Calculate cost of move for a shopping list.
     *
     * @param entityForSL the entity for sl
     * @param entityType entity type
     * @param provider the provider
     * @param commBoughtGroupingForSL the comm bought grouping
     * @return the cost
     */
    private float calculateMoveCost(@Nonnull final TopologyEntityDTO entityForSL,
            final int entityType, @Nullable TopologyDTO.TopologyEntityDTO provider,
            @Nonnull final CommoditiesBoughtFromProvider commBoughtGroupingForSL) {

        boolean shouldCalculate = !isPlan() && provider != null;

        // Cost of move for VM / Vol Storage consumer is determined by StorageAmount bought.
        if (shouldCalculate && provider.getEntityType() == EntityType.STORAGE_VALUE
                && (entityType == EntityType.VIRTUAL_MACHINE_VALUE
                || entityType == EntityType.VIRTUAL_VOLUME_VALUE)) {
            return storageMoveCostFactor * (float)(totalStorageAmountBought(commBoughtGroupingForSL)
                    / Units.KIBI);
        } else if (shouldCalculate && entityType == EntityType.VIRTUAL_MACHINE_VALUE && singleVMonHost
                && provider.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE
                && commBoughtGroupingForSL.getCommodityBoughtList().stream()
                    .filter(commBought -> commBought.getCommodityType().getType() == CommodityDTO.CommodityType.MEM_VALUE)
                    .anyMatch(commBought -> {
                        return numConsumersOfSoldCommTable.get(provider.getOid(), commBought.getCommodityType()) == 1
                                && provider.getCommoditySoldListList().stream()
                                .anyMatch(commSold -> {
                                    return commSold.getCommodityType().getType() == commBought.getCommodityType().getType()
                                            && (commBought.hasHistoricalUsed()
                                            ? commBought.getHistoricalUsed().getHistUtilization()
                                            / commSold.getCapacity() >= customUtilizationThreshold
                                            : commBought.getUsed() / commSold.getCapacity() >= customUtilizationThreshold);
                                });
                    })) {
                // Cost of move for a single VM on host is being set to a high value when the VM
                // consumes most of the host's capacity for Memory to avoid disruptive moves due to
                // potential fluctuations in memory observed and potential overhead.

                logger.info("Setting high cost of move for " + entityForSL.getDisplayName());
                return 10000000f;
        }

        return 0.0f;
    }

    /**
     * Get list of entity ids with delete actions generated from
     * {@link com.vmturbo.market.runner.wasted.WastedEntityResults}.
     *
     * @param entityTypes Currently set using {@link #ENTITY_TYPES_WITH_DELETE_ACTIONS}
     * @return list of entity ids.
     */
    private Collection<Long> extractWastedEntityIds(@Nonnull final Collection<Integer> entityTypes) {
        final Collection<Action> actions = new HashSet<>();
        wastedEntityResults.forEach(result -> {
            for (Integer entityType : entityTypes) {
                actions.addAll(result.getActionsByEntityType(entityType));
            }
        });
        return actions.stream().map(WastedEntityResults::getEntityIdFromAction).collect(Collectors.toSet());
    }

    /**
     * Set {@link WastedFilesResults} : Results from
     * delete actions on various entityTypes generated from {@link Analysis#execute()}.
     * See usage of {@link #ENTITY_TYPES_WITH_DELETE_ACTIONS} for how we extract entities
     * related to delete actions.
     *
     * @param result list of {@link WastedEntityResults}.
     */
    public void addAllWastedEntityResults(Collection<WastedEntityResults> result) {
        wastedEntityResults.addAll(result);
    }
}
