package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.TopologyDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.settings.EntitySettings;
import com.vmturbo.market.settings.MarketSettings;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults.NewShoppingListToBuyerEntry;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
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

    private static final Set<Integer> CONTAINER_TYPES = ImmutableSet.of(
        // TODO: Add container collection
        EntityType.CONTAINER_VALUE);

    private static final Logger logger = LogManager.getLogger();

    // TODO: In legacy this is taken from LicenseManager and is currently false
    private boolean includeGuaranteedBuyer = INCLUDE_GUARANTEED_BUYER_DEFAULT;

    private CloudTopologyConverter cloudTc;

    /**
     * Entities that are providers of containers.
     * Populated only for plans. For realtime market, this set will be empty.
     */
    private Set<Long> providersOfContainers = Sets.newHashSet();

    /**
     * Map from entity OID to entity.
     */
    private Map<Long, TopologyDTO.TopologyEntityDTO> entityOidToDto = Maps.newHashMap();

    private Map<Long, TopologyEntityDTO> unmodifiableEntityOidToDtoMap
            = Collections.unmodifiableMap(entityOidToDto);

    // Store skipped service entities which need to be added back to projected topology and price
    // index messages.
    private List<TopologyEntityDTO> skippedEntities = new ArrayList<>();

    // a map keeps shoppinglist oid to ShoppingListInfo which is a container for
    // shoppinglist oid, buyer oid, seller oid and commodity bought
    private final Map<Long, ShoppingListInfo> shoppingListOidToInfos = Maps.newHashMap();

    private final Map<EconomyCommodityId, CommodityType>
            commoditySpecMap = Maps.newHashMap();

    private long shoppingListId = 1000L; // Arbitrary start value

    private AtomicLong cloneIndex = new AtomicLong(0);

    private final NumericIDAllocator commodityTypeAllocator = new NumericIDAllocator();

    // a map to keep the oid to traderTO mapping, it also includes newly cloned traderTO
    private final Map<Long, EconomyDTOs.TraderTO> oidToTraderTOMap = Maps.newHashMap();

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

    private float quoteFactor = AnalysisUtil.QUOTE_FACTOR;
    private boolean isAlleviatePressurePlan = false;

    // Add a cost of moving from source to destination.
    public static final float MOVE_COST_FACTOR = 0.005f;
    public static final float PLAN_MOVE_COST_FACTOR = 0.0f;
    public static final float CLOUD_QUOTE_FACTOR = 1;

    private final CommodityConverter commodityConverter;

    private final Map<Long, TopologyEntityDTO> traderTOOidToTopologyEntityDTO = new HashMap<>();

    private final ActionInterpreter actionInterpreter;

    /**
     * A non-shop-together TopologyConverter.
     *
     * @param topologyInfo Information about the topology.
     */
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo, MarketPriceTable marketPriceTable) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.commodityConverter = new CommodityConverter(commodityTypeAllocator, commoditySpecMap,
                includeGuaranteedBuyer, dsBasedBicliquer, numConsumersOfSoldCommTable);
        this.cloudTc = new CloudTopologyConverter(unmodifiableEntityOidToDtoMap, topologyInfo,
                pmBasedBicliquer, dsBasedBicliquer, commodityConverter, azToRegionMap, businessAccounts, marketPriceTable);
        this.actionInterpreter = new ActionInterpreter(commodityConverter, shoppingListOidToInfos,
                cloudTc, unmodifiableEntityOidToDtoMap, oidToTraderTOMap);
    }

    /**
     * Constructor with includeGuaranteedBuyer parameter.
     *
     * @param topologyInfo Information about the topology.
     * @param includeGuaranteedBuyer whether to include guaranteed buyers (VDC, VPod, DPod) or not
     * @param quoteFactor to be used by move recommendations.
     */
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             final boolean includeGuaranteedBuyer,
                             final float quoteFactor,
                             @Nonnull final MarketPriceTable marketPriceTable) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
        this.quoteFactor  = quoteFactor;
        isAlleviatePressurePlan = TopologyDTOUtil.isAlleviatePressurePlan(topologyInfo);
        this.commodityConverter = new CommodityConverter(commodityTypeAllocator, commoditySpecMap,
                includeGuaranteedBuyer, dsBasedBicliquer, numConsumersOfSoldCommTable);
        this.cloudTc = new CloudTopologyConverter(unmodifiableEntityOidToDtoMap, topologyInfo,
                pmBasedBicliquer, dsBasedBicliquer, commodityConverter, azToRegionMap, businessAccounts,
                marketPriceTable);
        this.actionInterpreter = new ActionInterpreter(commodityConverter, shoppingListOidToInfos,
                cloudTc, unmodifiableEntityOidToDtoMap, oidToTraderTOMap);
    }

    @VisibleForTesting
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo,
                             final boolean includeGuaranteedBuyer,
                             final float quoteFactor, @Nonnull final MarketPriceTable marketPriceTable,
                             @Nonnull CommodityConverter commodityConverter) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
        this.quoteFactor  = quoteFactor;
        isAlleviatePressurePlan = TopologyDTOUtil.isAlleviatePressurePlan(topologyInfo);
        this.commodityConverter = commodityConverter;
        this.cloudTc = new CloudTopologyConverter(unmodifiableEntityOidToDtoMap, topologyInfo,
                pmBasedBicliquer, dsBasedBicliquer, commodityConverter, azToRegionMap, businessAccounts,
                marketPriceTable);
        this.actionInterpreter = new ActionInterpreter(commodityConverter, shoppingListOidToInfos,
                cloudTc, unmodifiableEntityOidToDtoMap, oidToTraderTOMap);
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
        for (TopologyDTO.TopologyEntityDTO entity : topology.values()) {
            int entityType = entity.getEntityType();
            if (AnalysisUtil.SKIPPED_ENTITY_TYPES.contains(entityType)
                || SKIPPED_ENTITY_STATES.contains(entity.getEntityState())
                || !includeByType(entityType)) {
                skippedEntities.add(entity);
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
                    vms.forEach(vm -> cloudEntityToBusinessAccount.put(vm, entity));
                    dbs.forEach(db -> cloudEntityToBusinessAccount.put(db, entity));
                    businessAccounts.add(entity);
                }
            }
        }
        return convertToMarket();
    }

    @Nonnull
    public List<TopologyDTO.TopologyEntityDTO> getSkippedEntities() {
        return Collections.unmodifiableList(skippedEntities);
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
                .filter(t -> !TopologyConversionConstants.CLOUD_ENTITY_TYPES_TO_SKIP_CONVERSION.contains(
                        t.getEntityType()))
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
        if (commSold.getCommodityType().getType() == CommodityDTO.CommodityType.DSPM_ACCESS_VALUE) {
            // Storage id first, PM id second.
            // This way each storage is a member of exactly one biclique.
            String dsOid = String.valueOf(dto.getOid());
            String pmOid = String.valueOf(commSold.getAccesses());
            dsBasedBicliquer.edge(dsOid, pmOid);
            // PM id first, storage id second.
            // This way each pm is a member of exactly one biclique.
            pmBasedBicliquer.edge(pmOid, dsOid);
        }
    }

    /**
     * Convert the {@link EconomyDTOs.TraderTO}s to {@link TopologyDTO.ProjectedTopologyEntity}s. This method
     * creates a lazy collection, which is really converted during iteration.
     *
     * @param projectedTraders list of {@link EconomyDTOs.TraderTO}s that are to be converted to
     * {@link TopologyDTO.TopologyEntityDTO}s
     * @param originalTopology the original set of {@link TopologyDTO.TopologyEntityDTO}s by OID.
     * @return list of {@link TopologyDTO.ProjectedTopologyEntity}s
     */
    @Nonnull
    public List<TopologyDTO.ProjectedTopologyEntity> convertFromMarket(
                @Nonnull final List<EconomyDTOs.TraderTO> projectedTraders,
                @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology,
                @Nonnull final PriceIndexMessage priceIndexMessage) {
        final Map<Long, PriceIndexMessagePayload> priceIndexByOid =
            priceIndexMessage.getPayloadList().stream()
                .collect(Collectors.toMap(PriceIndexMessagePayload::getOid, Function.identity()));
        logger.info("Converting projectedTraders to topologyEntityDTOs");
        projectedTraders.forEach(t -> oidToTraderTOMap.put(t.getOid(), t));
        // Perform lazy transformation, so do not store all the TopologyEntityDTOs in memory
        return Lists.transform(projectedTraders, projectedTrader -> {
            final TopologyDTO.TopologyEntityDTO projectedEntity =
                    traderTOtoTopologyDTO(projectedTrader, originalTopology);
            final ProjectedTopologyEntity.Builder projectedEntityBuilder =
                    ProjectedTopologyEntity.newBuilder()
                        .setEntity(projectedEntity);
            final PriceIndexMessagePayload priceIndex = priceIndexByOid.get(projectedEntity.getOid());
            if (priceIndex != null) {
                if (originalTopology.containsKey(projectedEntity.getOid())) {
                    projectedEntityBuilder.setOriginalPriceIndex(priceIndex.getPriceindexCurrent());
                }
                projectedEntityBuilder.setProjectedPriceIndex(priceIndex.getPriceindexProjected());
            }
            return projectedEntityBuilder.build();
        });
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
     * @param entityIdToType A map from entity ID to the type of that entity. The entities involved
     *                       in the action are expected to be in this map.
     * @param originalCloudTopology {@link CloudTopology} of the original {@link TopologyEntityDTO}s  received by Analysis
     * @param projectedCosts  A map of id of projected topologyEntityDTO -> {@link CostJournal} for the entity with that ID.
     *
     *
     * @return The {@link Action} describing the recommendation in a topology-specific way.
     */
    @Nonnull
    public Optional<Action> interpretAction(@Nonnull final ActionTO actionTO,
                                            @Nonnull final Map<Long, Integer> entityIdToType,
                                            @NonNull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                            @NonNull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
                                            @NonNull TopologyCostCalculator topologyCostCalculator) {
        return actionInterpreter.interpretAction(actionTO, entityIdToType, originalCloudTopology,
                projectedCosts, topologyCostCalculator);
    }

    /**
     * Convert a {@link EconomyDTOs.TraderTO} to a {@link TopologyDTO.TopologyEntityDTO}.
     *
     * @param traderTO {@link EconomyDTOs.TraderTO} that is to be converted to a {@link TopologyDTO.TopologyEntityDTO}
     * @param traderOidToEntityDTO whose key is the traderOid and the value is the original
     * traderTO
     * @return list of {@link TopologyDTO.TopologyEntityDTO}s
     */
    private TopologyDTO.TopologyEntityDTO traderTOtoTopologyDTO(EconomyDTOs.TraderTO traderTO,
                    @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> traderOidToEntityDTO) {
        if (cloudTc.isMarketTier(traderTO.getOid())) {
            return cloudTc.getMarketTier(traderTO.getOid()).getTier();
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
                    commBoughtTOtoCommBoughtDTO(commBought, originalEntity).ifPresent(commList::add);
                }
            }
            final CommoditiesBoughtFromProvider.Builder commoditiesBoughtFromProviderBuilder =
                TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder()
                    .addAllCommodityBought(commList);
            // if Market still can not find a placement, we should not set provider Id.
            if (sl.hasSupplier()) {
                Long supplier = sl.getSupplier();
                // If the supplier is a market tier, then get the tier TopologyEntityDTO and
                // make that the supplier
                if (cloudTc.isMarketTier(sl.getSupplier())) {
                    supplier = cloudTc.getMarketTier(sl.getSupplier()).getTier().getOid();
                }
                commoditiesBoughtFromProviderBuilder.setProviderId(supplier);
            }
            // if can not find the ShoppingListInfo, that means Market generate some wrong shopping
            // list.
            if (!shoppingListOidToInfos.containsKey(sl.getOid())) {
                throw new IllegalStateException("Market returned invalid shopping list for : " + sl);
            }
            shoppingListOidToInfos.get(sl.getOid()).getSellerEntityType()
                    .ifPresent(commoditiesBoughtFromProviderBuilder::setProviderEntityType);
            topoDTOCommonBoughtGrouping.add(commoditiesBoughtFromProviderBuilder.build());
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
            .build();
        TopologyDTO.TopologyEntityDTO.Builder entityDTO =
                TopologyDTO.TopologyEntityDTO.newBuilder()
                    .setEntityType(traderTO.getType())
                    .setOid(traderTO.getOid())
                    .setEntityState(entityState)
                    .setDisplayName(displayName)
                    .addAllCommoditySoldList(retrieveCommSoldList(traderTO, originalEntity))
                    .addAllCommoditiesBoughtFromProviders(topoDTOCommonBoughtGrouping)
                    .addAllConnectedEntityList(getConnectedEntities(traderTO))
                    .setAnalysisSettings(analysisSetting);
        if (originalEntity == null) {
            // this is a clone trader
            originalEntity = traderOidToEntityDTO.get(traderTO.getCloneOf());
        } else {
            // copy the origin story from the original entity
            if (originalEntity.hasOrigin()) {
                entityDTO.setOrigin(originalEntity.getOrigin());
            }
        }
        // get dspm and datastore commodity sold from the original trader, add
        // them to projected topology entity DTO
        return entityDTO.addAllCommoditySoldList(originalEntity.getCommoditySoldListList().stream()
                        .filter(c -> AnalysisUtil.DSPM_OR_DATASTORE
                                        .contains(c.getCommodityType().getType()))
                        .collect(Collectors.toSet()))
                        .build();
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
        if (cloudTc.isTraderConsumingFromMaketTier(traderTO)) {
            // Primary market tier refers to Compute market tier / database market tier
            MarketTier destinationPrimaryMarketTier = cloudTc.getPrimaryMarketTier(traderTO);
            if (destinationPrimaryMarketTier == null) {
                logger.error("Could not fetch primary market tier for {}",
                        traderTO.getDebugInfoNeverUseInCode());
                return connectedEntities;
            }
            TopologyEntityDTO originalCloudConsumer = traderTOOidToTopologyEntityDTO.get(traderTO.getOid());
            TopologyEntityDTO sourceRegion = cloudTc.getRegionOfCloudConsumer(originalCloudConsumer);
            if (sourceRegion == destinationPrimaryMarketTier.getRegion()) {
                // cloud consumer (VM / DB) did NOT move to a different region
                TopologyEntityDTO sourceAZ = cloudTc.getAZOfCloudConsumer(originalCloudConsumer);
                if (sourceAZ != null) {
                    ConnectedEntity az = ConnectedEntity.newBuilder().setConnectedEntityId(sourceAZ.getOid())
                            .setConnectedEntityType(sourceAZ.getEntityType())
                            .setConnectionType(ConnectionType.NORMAL_CONNECTION).build();
                    connectedEntities.add(az);
                }
            } else {
                // cloud consumer (VM / DB) has moved to a different region
                // Pick the first AZ in the destination region
                TopologyEntityDTO destAZ = TopologyDTOUtil.getConnectedEntitiesOfType(sourceRegion,
                        EntityType.AVAILABILITY_ZONE_VALUE, unmodifiableEntityOidToDtoMap).get(0);
                ConnectedEntity az = ConnectedEntity.newBuilder().setConnectedEntityId(destAZ.getOid())
                        .setConnectedEntityId(destAZ.getEntityType())
                        .setConnectionType(ConnectionType.NORMAL_CONNECTION).build();
                connectedEntities.add(az);
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
        // return an empty map if there is no trader in oidToTraderTOMap
        if (oidToTraderTOMap.isEmpty()) {
            return pMToSTMap;
        }
        List<Long> stList = new ArrayList<>();
        for (EconomyDTOs.ShoppingListTO sl : shoppingListList) {
            if (oidToTraderTOMap.containsKey(sl.getSupplier())) {
                long supplierType = oidToTraderTOMap.get(sl.getSupplier()).getType();
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
     * @param originalEntity the original TopologyEntityDTO corresponding to this DTO, if any, or null
     * @return list of {@link TopologyDTO.CommoditySoldDTO}s
     */
    private Set<TopologyDTO.CommoditySoldDTO> retrieveCommSoldList(
            @Nonnull final TraderTO traderTO,
            @Nullable final TopologyEntityDTO originalEntity) {
        return traderTO.getCommoditiesSoldList().stream()
                    .map(commoditySoldTO -> commSoldTOtoCommSoldDTO(commoditySoldTO,
                            originalEntity))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toSet());
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
     * @param originalEntity the original TopologyEntityDTO, used to look up the original scaleFactor,
     *                       if any, for this commodity
     * @return {@link TopologyDTO.CommoditySoldDTO} that the trader sells
     */
    @Nonnull
    private Optional<TopologyDTO.CommodityBoughtDTO> commBoughtTOtoCommBoughtDTO(
            @Nonnull final CommodityBoughtTO commBoughtTO, final TopologyEntityDTO originalEntity) {

        return commodityConverter.economyToTopologyCommodity(commBoughtTO.getSpecification())
                .map(commType -> TopologyDTO.CommodityBoughtDTO.newBuilder()
                    .setUsed(reverseScaleCommBought(commType, commBoughtTO.getQuantity(), originalEntity))
                    .setCommodityType(commType)
                    .setPeak(commBoughtTO.getPeakQuantity())
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
            boolean suspendable = EntitySettings.BooleanKey.ENABLE_SUSPEND.value(topologyDTO);
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
            boolean isEntityFromCloud = TopologyConversionUtils.isEntityConsumingCloud(topologyDTO);
            TraderSettingsTO.Builder settingsBuilder = TopologyConversionUtils.
                    createCommonTraderSettingsTOBuilder(
                            topologyDTO, unmodifiableEntityOidToDtoMap, isAlleviatePressurePlan);
            settingsBuilder.setClonable(clonable && topologyDTO.getAnalysisSettings().getControllable())
                    .setSuspendable(suspendable)
                    .setCanAcceptNewCustomers(topologyDTO.getAnalysisSettings().getIsAvailableAsProvider()
                                              && topologyDTO.getAnalysisSettings().getControllable())
                    .setIsEligibleForResizeDown(isPlan() ||
                            topologyDTO.getAnalysisSettings().getIsEligibleForResizeDown())
                    .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                            .setSumOfCommodity(SumOfCommodity.getDefaultInstance()))
                    .setQuoteFactor(isEntityFromCloud ? CLOUD_QUOTE_FACTOR :quoteFactor)
                    .setMoveCostFactor((isPlan() || isEntityFromCloud) ? PLAN_MOVE_COST_FACTOR : MOVE_COST_FACTOR);
            if (cloudEntityToBusinessAccount.get(topologyDTO) != null) {
                settingsBuilder.setBalanceAccount(createBalanceAccountDTO(topologyDTO));
            }
            final EconomyDTOs.TraderSettingsTO settings = settingsBuilder.build();

            //compute biclique IDs for this entity, the clique list will be used only for
            // shop together placement, so pmBasedBicliquer is called
            Set<Long> allCliques = pmBasedBicliquer.getBcIDs(String.valueOf(topologyDTO.getOid()));

            // In a headroom plan, the only modifications to the topology are additions of clones.
            // Clones are always unplaced when they are first created.
            // We assume that the "default" topology has no unplaced entities. Therefore, any
            // unplaced entities are going to be the clones created for headroom calculation
            // purposes.
            final boolean isTemplateForHeadroom =
                    TopologyDTOUtil.isPlanType(PlanProjectType.CLUSTER_HEADROOM, topologyInfo) &&
                    !TopologyDTOUtil.isPlaced(topologyDTO);

            traderDTO = EconomyDTOs.TraderTO.newBuilder()
                    // Type and Oid are the same in the topology DTOs and economy DTOs
                    .setOid(topologyDTO.getOid())
                    .setType(entityType)
                    .setState(state)
                    .setSettings(settings)
                    .setTemplateForHeadroom(isTemplateForHeadroom)
                    .setDebugInfoNeverUseInCode(entityDebugInfo(topologyDTO))
                    .addAllCommoditiesSold(createAllCommoditySoldTO(topologyDTO))
                    .addAllShoppingLists(createAllShoppingLists(topologyDTO))
                    .addAllCliques(allCliques)
                    .build();
            traderTOOidToTopologyEntityDTO.put(traderDTO.getOid(), topologyDTO);
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
        final boolean shopTogether = isAlleviatePressurePlan ||
                topologyDTO.getAnalysisSettings().getShopTogether();
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
                        topologyEntity.getOid(),
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
     * @param buyerOid the OID of the buyer of the shopping list
     * @param entityType the entity type of the buyer
     * @param shopTogether whether the entity supports the shop-together feature
     * @param providerOid the oid of the seller of the shopping list
     * @param commBoughtGrouping the commodities bought group by the buyer from the provider
     * @param providers a map that captures the previous placement for unplaced plan entities
     * @return a shopping list between the buyer and seller
     */
    @Nonnull
    private EconomyDTOs.ShoppingListTO createShoppingList(
            final long buyerOid,
            final int entityType,
            final boolean shopTogether,
            @Nullable final Long providerOid,
            @Nonnull final CommoditiesBoughtFromProvider commBoughtGrouping,
            final Map<Long, Long> providers) {
        TopologyDTO.TopologyEntityDTO provider = (providerOid != null) ? entityOidToDto.get(providerOid) : null;
        float moveCost = !isPlan() && (entityType == EntityType.VIRTUAL_MACHINE_VALUE
                && provider != null // this check is for testing purposes
                && provider.getEntityType() == EntityType.STORAGE_VALUE)
                        ? (float)(totalStorageAmountBought(buyerOid) / Units.KIBI)
                        : 0.0f;
        Set<CommodityDTOs.CommodityBoughtTO> values = commBoughtGrouping.getCommodityBoughtList()
            .stream()
            .filter(CommodityBoughtDTO::getActive)
            .map(topoCommBought -> convertCommodityBought(buyerOid, topoCommBought, providerOid,
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
        }
        final long id = shoppingListId++;
        final boolean isMovable = commBoughtGrouping.hasMovable()
            ? commBoughtGrouping.getMovable()
            : AnalysisUtil.MOVABLE_TYPES.contains(entityType);
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
        shoppingListOidToInfos.put(id,
            new ShoppingListInfo(id, buyerOid, providerOid, providerEntityType,
                    commBoughtGrouping.getCommodityBoughtList()));
        return economyShoppingListBuilder.build();
    }

    /**
     * Creates a DC Comm bought for a cloud entity which has a provider as a Compute tier.
     *
     * @param providerOid oid of the market tier provider oid
     * @param buyerOid oid of the buyer of the shopping list
     * @return The commodity bought TO
     */
    @Nullable
    private Optional<CommodityBoughtTO> createDCCommodityBoughtForCloudEntity(
            long providerOid, long buyerOid) {
        MarketTier marketTier = cloudTc.getMarketTier(providerOid);
        int providerEntityType = marketTier.getTier().getEntityType();
        CommodityBoughtTO dcCommBought = null;
        if (providerEntityType == EntityType.COMPUTE_TIER_VALUE) {
            TopologyEntityDTO region = cloudTc.getRegionOfCloudConsumer(entityOidToDto.get(buyerOid));
            List<CommoditySoldDTO> dcCommSoldList = region.getCommoditySoldListList().stream()
                    .filter(c -> c.getCommodityType().getType()
                            == CommodityDTO.CommodityType.DATACENTER_VALUE
                            && c.getCommodityType().hasKey())
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            if (dcCommSoldList.size() != 1) {
                logger.error("{} is selling {} DC Commodities - {}", region.getDisplayName(),
                        dcCommSoldList.size(), dcCommSoldList.stream().map(
                                c -> c.getCommodityType().getKey())
                                .collect(Collectors.joining(",")));
                return Optional.empty();
            }
            CommoditySoldDTO dcCommSold = dcCommSoldList.get(0);
            dcCommBought = CommodityDTOs.CommodityBoughtTO.newBuilder()
                    .setSpecification(commodityConverter.commoditySpecification(
                            dcCommSold.getCommodityType()))
                    .build();
        }
        return Optional.ofNullable(dcCommBought);
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
     * @param buyerOid the OID of the buyer (presumably a VM)
     * @return total used storage amount bought
     */
    private double totalStorageAmountBought(long buyerOid) {
        return entityOidToDto.get(buyerOid).getCommoditiesBoughtFromProvidersList().stream()
            .filter(this::isStorage)
            .map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
            .flatMap(List::stream)
            .filter(this::isStorageAmount)
            .mapToDouble(TopologyDTO.CommodityBoughtDTO::getUsed)
            .sum();
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
        if (providerTopologyEntity != null && TopologyConversionConstants.TIER_ENTITY_TYPES
                .contains(providerTopologyEntity.getEntityType())) {
            // Provider is a compute tier / storage tier / database tier
            // Get the region connected to the topologyEntity
            TopologyEntityDTO region = cloudTc.getRegionOfCloudConsumer(topologyEntity);
            providerId = cloudTc.getTraderTOOid(new OnDemandMarketTier(
                    providerTopologyEntity, region));
        }
        return providerId;
    }

    /**
     * Checks whether the commodity bought is a StorageAmount.
     *
     * @param comm a commodity bought
     * @return whether the commodity type is StorageAmount
     */
    private boolean isStorageAmount(TopologyDTO.CommodityBoughtDTO comm) {
        return comm.getCommodityType().getType() == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE;
    }

    private CommodityDTOs.CommodityBoughtTO convertCommodityBought(
            long buyerOid, @Nonnull final TopologyDTO.CommodityBoughtDTO topologyCommBought,
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
            : createAndValidateCommBoughtTO(topologyCommBought);
    }

    private CommodityDTOs.CommodityBoughtTO createAndValidateCommBoughtTO(
                    TopologyDTO.CommodityBoughtDTO topologyCommBought) {
        float peakQuantity = (float)topologyCommBought.getPeak();
        float usedQuantity = (float)topologyCommBought.getUsed();

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
                .setQuantity(usedQuantity)
                .setPeakQuantity(peakQuantity)
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
     * @param commSoldTO the market CommdditySoldTO to convert
     * @param originalEntity the original {@link TopologyEntityDTO} XL model
     *                       object received by market as part of the broadcast
     *                       topology to analyze
     * @return a {@link CommoditySoldDTO} equivalent to the original.
     */
    @Nonnull
    private Optional<TopologyDTO.CommoditySoldDTO> commSoldTOtoCommSoldDTO(
            @Nonnull final CommoditySoldTO commSoldTO,
            @Nullable final TopologyEntityDTO originalEntity) {
        return commodityConverter.economyToTopologyCommodity(commSoldTO.getSpecification())
                .map(commType -> CommoditySoldDTO.newBuilder()
                        .setCapacity(reverseScaleCommSold(commType, commSoldTO.getCapacity(),
                                originalEntity))
                        .setUsed(reverseScaleCommSold(commType, commSoldTO.getQuantity(),
                                originalEntity))
                        .setPeak(commSoldTO.getPeakQuantity())
                        .setMaxQuantity(commSoldTO.getMaxQuantity())
                        .setIsResizeable(commSoldTO.getSettings().getResizable())
                        .setEffectiveCapacityPercentage(
                                commSoldTO.getSettings().getUtilizationUpperBound() * 100)
                        .setCommodityType(commType)
                        .setIsThin(commSoldTO.getThin())
                        .setCapacityIncrement(
                                commSoldTO.getSettings().getCapacityIncrement())
                        .build());
    }

    /**
     * If this commodity-sold in the original {@link TopologyEntityDTO} in the input topology
     * had a scale factor, then reverse the scaling on the way out. In other words, since
     * we multiplied by the scale factor on the way in to the market analysis, here we
     * divide by the scale factor on the way back out.
     * <p/>
     * Note that if there is a scale factor but that scale factor is zero, which should never happen,
     * we simply return the originalValue, i.e. we do _not_ return Inf or NaN.
     * <p/>
     * Note that to perform the reverse scale we must find the 'original' commodity by type.
     * If are were multiple commodities of the same type, e.g. differentiated by 'key' values, in the
     * original TopologyEntityDTO then this search will only find the first one. This is unavoidable
     * as the CommoditySoldTO for the market does not record the 'key' value from the
     * original commodity.
     *
     * @param commType the type of commodity being processed, used to find the 'original'
     *                 commodity value
     * @param valueToReverseScale the commodity value output from the market to
     *                            scale down (if there is a scaleFactor)
     * @param originalEntity the input to the MarketAnalysis, used to look up the original
     *                       commodity and find the 'scaleFactor', if any
     * @return either the valueToReverseScale divided by the scaleFactor if the original
     * commodity had defined a scaleFactor, else the valueToReverseScale unmodified
     */
    private double reverseScaleCommSold(CommodityType commType, float valueToReverseScale,
                                        @Nullable final TopologyEntityDTO originalEntity) {
        if (originalEntity == null) {
            return valueToReverseScale;
        }
        return originalEntity.getCommoditySoldListList().stream()
                // look for the corresponding commodity on the original TopologyEntityDTO; also
                // require that the original has a 'scalingFactor' set and is non-zero
                .filter(originalCommodity ->
                        originalCommodity.getCommodityType().equals(commType) &&
                                originalCommodity.hasScalingFactor() &&
                                originalCommodity.getScalingFactor() != 0)
                .findFirst()
                .map(matchingCommodity -> {
                    // found the matching commodity - divide by scalingFactor
                    if (logger.isDebugEnabled()) {
                        logger.debug("reverse scale sold comm {} for {} mkt val {}, factor {}, unscaled {}",
                                commType, originalEntity.getDisplayName(), valueToReverseScale,
                                matchingCommodity.getScalingFactor(),
                                valueToReverseScale / matchingCommodity.getScalingFactor());
                    }
                    return valueToReverseScale / matchingCommodity.getScalingFactor();
                })
                // either we didn't find the matching commodity, or the matching commodity had
                // no 'scalingFactor', or the 'scalingFactor' was zero (and hence we could not divide)
                .orElse((double) valueToReverseScale);
    }

    /**
     * If this commodity-bought in the original {@link TopologyEntityDTO} in the input topology
     * had a scale factor, then reverse the scaling on the way out. In other words, since
     * we multiplied by the scale factor on the way in to the market analysis, here we
     * divide by the scale factor on the way back out.
     * <p/>
     * Note that if there is a scale factor but that scale factor is zero, which should never happen,
     * we simply return the originalValue, i.e. we do _not_ return Inf or NaN.
     * <p/>
     * Note that to perform the reverse scale we must find the 'original' commodity by type.
     * If are were multiple commodities of the same type, e.g. differentiated by 'key' values, in the
     * original TopologyEntityDTO then this search will only find the first one. This is unavoidable
     * as the CommoditySoldTO for the market does not record the 'key' value from the
     * original commodity.
     *
     * @param commType the type of commodity being processed, used to find the 'original'
     *                 commodity value
     * @param valueToReverseScale the commodity value output from the market to
     *                            scale down (if there is a scaleFactor)
     * @param originalEntity the input to the MarketAnalysis, used to look up the original
     *                       commodity and find the 'scaleFactor', if any
     * @return either the valueToReverseScale divided by the scaleFactor if the original
     * commodity had defined a scaleFactor, else the valueToReverseScale unmodified
     */
    private double reverseScaleCommBought(CommodityType commType, float valueToReverseScale,
                                          @Nullable final TopologyEntityDTO originalEntity) {
        if (originalEntity == null) {
            return valueToReverseScale;
        }
        return originalEntity.getCommoditiesBoughtFromProvidersList().stream()
                .flatMap(commoditiesBoughtFromProvider -> commoditiesBoughtFromProvider.getCommodityBoughtList().stream())
                .filter(originalCommodity -> originalCommodity.getCommodityType().equals(commType) &&
                        originalCommodity.hasScalingFactor() &&
                        originalCommodity.getScalingFactor() != 0)
                .findFirst()
                .map(matchingCommodity -> {
                    // found the matching commodity - divide by scalingFactor
                    if (logger.isDebugEnabled()) {
                        logger.debug("reverse scale bought comm {} for {} mkt val {}, factor {}, unscaled {}",
                                commType, originalEntity.getDisplayName(), valueToReverseScale,
                                matchingCommodity.getScalingFactor(),
                                valueToReverseScale / matchingCommodity.getScalingFactor());
                    }
                    return valueToReverseScale / matchingCommodity.getScalingFactor();
                })
                .orElse((double) valueToReverseScale);
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
                    new ShoppingListInfo(l.getNewShoppingList(), l.getBuyer(), null,
                            null, Lists.newArrayList())));
    }
}
