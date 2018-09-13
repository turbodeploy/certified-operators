package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.settings.EntitySettings;
import com.vmturbo.market.settings.MarketSettings;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.CompoundMoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults.NewShoppingListToBuyerEntry;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessagePayload;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.SumOfCommodity;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Convert topology DTOs to economy DTOs.
 */
public class TopologyConverter {

    private static final String BICLIQUE = "BICLIQUE";

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
    public static final String COMMODITY_TYPE_KEY_SEPARATOR = "|";

    // TODO: In legacy this is taken from LicenseManager and is currently false
    private boolean includeGuaranteedBuyer = INCLUDE_GUARANTEED_BUYER_DEFAULT;

    /**
     * Entities that are providers of containers.
     * Populated only for plans. For realtime market, this set will be empty.
     */
    private Set<Long> providersOfContainers = Sets.newHashSet();

    /**
     * Map from entity OID to entity.
     */
    private Map<Long, TopologyDTO.TopologyEntityDTO> entityOidToDto = Maps.newHashMap();

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
    private Map<Long, EconomyDTOs.TraderTO> oidToTraderTOMap = Maps.newHashMap();

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

    private final TopologyInfo topologyInfo;

    private float quoteFactor = AnalysisUtil.QUOTE_FACTOR;
    private boolean isAlleviatePressurePlan = false;

    public static final float CAPACITY_FACTOR = 0.999999f;

    public static final float MIN_DESIRED_UTILIZATION_VALUE = 0.0f;
    public static final float MAX_DESIRED_UTILIZATION_VALUE = 1.0f;



    /**
     * A non-shop-together TopologyConverter.
     *
     * @param topologyInfo Information about the topology.
     */
    public TopologyConverter(@Nonnull final TopologyInfo topologyInfo) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
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
                             final float quoteFactor) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
        this.quoteFactor  = quoteFactor;
        isAlleviatePressurePlan = TopologyDTOUtil.isAlleviatePressurePlan(topologyInfo);
    }

    private boolean isPlan() {
        return TopologyDTOUtil.isPlan(topologyInfo);
    }

    /**
     * Convert a collection of common protobuf topology entity DTOs to analysis protobuf economy DTOs.
     * @param topology list of topology entity DTOs
     * @return set of economy DTOs
     * @throws InvalidTopologyException when the topology is invalid, e.g. used > capacity
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
                continue;
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
     * @throws InvalidTopologyException when the topology is invalid, e.g. used > capacity
     */
    @Nonnull
    private Set<EconomyDTOs.TraderTO> convertToMarket() {
        logger.info("Converting topologyEntityDTOs to traderTOs");
        logger.debug("Start creating bicliques");
        BiMap<Long, String> oidToUuidMap = HashBiMap.create();
        for (TopologyDTO.TopologyEntityDTO dto : entityOidToDto.values()) {
            dto.getCommoditySoldListList().stream()
                .filter(comm -> isBicliqueCommodity(comm.getCommodityType()))
                .forEach(comm -> edge(dto, comm));
            oidToUuidMap.put(dto.getOid(), String.valueOf(dto.getOid()));
            populateCommodityConsumesTable(dto);
        }
        dsBasedBicliquer.compute(oidToUuidMap);
        pmBasedBicliquer.compute(oidToUuidMap);
        logger.debug("Done creating bicliques");

        final ImmutableSet.Builder<EconomyDTOs.TraderTO> returnBuilder = ImmutableSet.builder();
        entityOidToDto.values().stream()
                .map(this::topologyDTOtoTraderTO)
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
                    consumersCount = new Integer(0);
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
        oidToTraderTOMap = projectedTraders.stream().collect(
                Collectors.toMap(EconomyDTOs.TraderTO::getOid, Function.identity()));
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
     * Convert a {@link EconomyDTOs.TraderTO} to a {@link TopologyDTO.TopologyEntityDTO}.
     *
     * @param traderTO {@link EconomyDTOs.TraderTO} that is to be converted to a {@link TopologyDTO.TopologyEntityDTO}
     * @param traderOidToEntityDTO whose key is the traderOid and the value is the original
     * traderTO
     * @return list of {@link TopologyDTO.TopologyEntityDTO}s
     */
    private TopologyDTO.TopologyEntityDTO traderTOtoTopologyDTO(EconomyDTOs.TraderTO traderTO,
                    @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> traderOidToEntityDTO) {
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
        for (EconomyDTOs.ShoppingListTO sl : traderTO.getShoppingListsList()) {
            List<TopologyDTO.CommodityBoughtDTO> commList =
                            new ArrayList<TopologyDTO.CommodityBoughtDTO>();
            int bicliqueBaseType = commodityTypeAllocator.allocate(BICLIQUE);
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
                    commBoughtTOtoCommBoughtDTO(commBought).ifPresent(commList::add);
                }
            }
            final CommoditiesBoughtFromProvider.Builder commoditiesBoughtFromProviderBuilder =
                TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder()
                    .addAllCommodityBought(commList);
            // if Market still can not find a placement, we should not set provider Id.
            if (sl.hasSupplier()) {
                commoditiesBoughtFromProviderBuilder.setProviderId(sl.getSupplier());
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
        TopologyDTO.TopologyEntityDTO originalEntity = traderOidToEntityDTO.get(traderTO.getOid());
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
                    .addAllCommoditySoldList(retrieveCommSoldList(traderTO))
                    .addAllCommoditiesBoughtFromProviders(topoDTOCommonBoughtGrouping)
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
        List<Long> stList = new ArrayList<Long>();
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
     * @param traderTO {@link EconomyDTOs.TraderTO} whose commoditiesSold are to be converted into DTOs
     * @return list of {@link TopologyDTO.CommoditySoldDTO}s
     */
    private Set<TopologyDTO.CommoditySoldDTO> retrieveCommSoldList(
                    @Nonnull final EconomyDTOs.TraderTO traderTO) {
        return traderTO.getCommoditiesSoldList().stream()
                    .map(this::commSoldTOtoCommSoldDTO)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toSet());
    }

    /**
     * Convert {@link CommodityDTOs.CommodityBoughtTO} of a trader to its corresponding
     * {@link TopologyDTO.CommodityBoughtDTO}.
     *
     * @param commBoughtTO {@link CommodityDTOs.CommodityBoughtTO} that is to be converted to
     * {@link TopologyDTO.CommodityBoughtDTO}
     * @return {@link TopologyDTO.CommoditySoldDTO} that the trader sells
     */
    @Nonnull
    private Optional<TopologyDTO.CommodityBoughtDTO> commBoughtTOtoCommBoughtDTO(
            @Nonnull final CommodityDTOs.CommodityBoughtTO commBoughtTO) {
        return economyToTopologyCommodity(commBoughtTO.getSpecification())
                .map(commType -> TopologyDTO.CommodityBoughtDTO.newBuilder()
                    .setUsed(commBoughtTO.getQuantity())
                    .setCommodityType(commType)
                    .setPeak(commBoughtTO.getPeakQuantity())
                    .build());
    }

    private EconomyDTOs.TraderTO topologyDTOtoTraderTO(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO) {
        EconomyDTOs.TraderTO traderDTO = null;
        try {
            final boolean shopTogether = isAlleviatePressurePlan ? true
                            : topologyDTO.getAnalysisSettings().getShopTogether();
            final EconomyDTOs.TraderStateTO state = traderState(topologyDTO);
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
            final boolean isGuranteedBuyer = guaranteedBuyer(topologyDTO);
            final EconomyDTOs.TraderSettingsTO settings = EconomyDTOs.TraderSettingsTO.newBuilder()
                    .setClonable(clonable && topologyDTO.getAnalysisSettings().getControllable())
                    .setSuspendable(suspendable)
                    .setMinDesiredUtilization(getMinDesiredUtilization(topologyDTO))
                    .setMaxDesiredUtilization(getMaxDesiredUtilization(topologyDTO))
                    .setGuaranteedBuyer(isGuranteedBuyer)
                    .setCanAcceptNewCustomers(topologyDTO.getAnalysisSettings().getIsAvailableAsProvider()
                                              && topologyDTO.getAnalysisSettings().getControllable())
                    .setIsShopTogether(shopTogether)
                    .setIsEligibleForResizeDown(isPlan() ||
                            topologyDTO.getAnalysisSettings().getIsEligibleForResizeDown())
                    .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                            .setSumOfCommodity(SumOfCommodity.getDefaultInstance()))
                    .setQuoteFactor(quoteFactor)
                    .build();

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
                    .addAllCommoditiesSold(commoditiesSoldList(topologyDTO))
                    .addAllShoppingLists(createAllShoppingLists(topologyDTO))
                    .addAllCliques(allCliques)
                    .build();
        } catch (Exception e) {
            logger.error(entityDebugInfo(topologyDTO) + " could not be converted to traderTO:", e);
        }
        return traderDTO;
    }

    @VisibleForTesting
    static float getMinDesiredUtilization(
            @Nonnull final TopologyEntityDTO topologyDTO) {

        final TopologyEntityDTO.AnalysisSettings analysisSettings =
            topologyDTO.getAnalysisSettings();

        if (analysisSettings.hasDesiredUtilizationTarget() &&
                analysisSettings.hasDesiredUtilizationRange()) {

            return limitFloatRange((analysisSettings.getDesiredUtilizationTarget()
                    - (analysisSettings.getDesiredUtilizationRange() / 2.0f)) / 100.0f,
                    MIN_DESIRED_UTILIZATION_VALUE, MAX_DESIRED_UTILIZATION_VALUE);
        } else {
            return EntitySettings.NumericKey.DESIRED_UTILIZATION_MIN.value(topologyDTO);
        }
    }

    @VisibleForTesting
    static float getMaxDesiredUtilization(
            @Nonnull final TopologyEntityDTO topologyDTO) {

        final TopologyEntityDTO.AnalysisSettings analysisSettings =
            topologyDTO.getAnalysisSettings();

        if (analysisSettings.hasDesiredUtilizationTarget() &&
                analysisSettings.hasDesiredUtilizationRange()) {

            return limitFloatRange((analysisSettings.getDesiredUtilizationTarget()
                    + (analysisSettings.getDesiredUtilizationRange() / 2.0f)) / 100.0f,
                    MIN_DESIRED_UTILIZATION_VALUE, MAX_DESIRED_UTILIZATION_VALUE);
        } else {
            return EntitySettings.NumericKey.DESIRED_UTILIZATION_MAX.value(topologyDTO);
        }
    }

    public static float limitFloatRange(float value, float min, float max) {
        Preconditions.checkArgument(min <= max,
            "Min: %s must be <= max: %s", min, max);
        return Math.min(max, Math.max(value, min));
    }

    /**
     * An entity is a guaranteed buyer if it is a VDC that consumes (directly) from
     * storage or PM, or if it is a DPod.
     *
     * @param topologyDTO the entity to examine
     * @return whether the entity is a guaranteed buyer
     */
    private boolean guaranteedBuyer(TopologyDTO.TopologyEntityDTO topologyDTO) {
        int entityType = topologyDTO.getEntityType();
        return (entityType == EntityType.VIRTUAL_DATACENTER_VALUE)
                        && topologyDTO.getCommoditiesBoughtFromProvidersList()
                            .stream()
                            .filter(CommoditiesBoughtFromProvider::hasProviderId)
                            .map(CommoditiesBoughtFromProvider::getProviderId)
                            .collect(Collectors.toSet())
                        .stream()
                        .map(entityOidToDto::get)
                        .map(TopologyDTO.TopologyEntityDTO::getEntityType)
                        .anyMatch(type -> AnalysisUtil.GUARANTEED_SELLER_TYPES.contains(type))
                    || entityType == EntityType.DPOD_VALUE;
    }

    /**
     * Construct a string that can be used for debug purposes.
     * @param topologyDTO the topology entity DTO
     * @return a string in the format "VIRTUAL_MACHINE::VM-1" where "VM-1" is the entity
     * display name.
     */
    @Nonnull
    private static String entityDebugInfo(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO) {
        return EntityType.forNumber(topologyDTO.getEntityType())
                + "::"
                + topologyDTO.getDisplayName();
    }

    @Nonnull
    private EconomyDTOs.TraderStateTO traderState(
            @Nonnull final TopologyEntityDTO entity) {
        EntityState entityState = entity.getEntityState();
        return entityState == TopologyDTO.EntityState.POWERED_ON
                ? EconomyDTOs.TraderStateTO.ACTIVE
                : entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                    ? EconomyDTOs.TraderStateTO.IDLE
                    : EconomyDTOs.TraderStateTO.INACTIVE;
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
            .filter(commBoughtGrouping -> includeByType(commBoughtGrouping.getProviderEntityType()))
            .map(commBoughtGrouping -> createShoppingList(
                    topologyEntity.getOid(),
                    topologyEntity.getEntityType(),
                    topologyEntity.getAnalysisSettings().getShopTogether(),
                    getProviderId(commBoughtGrouping),
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
            .filter(topoCommBought -> topoCommBought.getActive())
            .map(topoCommBought -> convertCommodityBought(topoCommBought, providerOid, shopTogether, providers))
            .filter(comm -> comm != null) // Null for DSPMAccess/Datastore and shop-together
            .collect(Collectors.toSet());
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
    private Long getProviderId(CommoditiesBoughtFromProvider commodityBoughtGrouping) {
        return commodityBoughtGrouping.hasProviderId() ?
            commodityBoughtGrouping.getProviderId() :
            null;
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
            @Nonnull final TopologyDTO.CommodityBoughtDTO topologyCommBought,
            @Nullable final Long providerOid,
            final boolean shopTogether,
            final Map<Long, Long> providers) {
        CommodityType type = topologyCommBought.getCommodityType();
        return isBicliqueCommodity(type)
            ? shopTogether
                // skip DSPMAcess and Datastore commodities in the case of shop-together
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
                .setSpecification(commoditySpecification(topologyCommBought.getCommodityType()))
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
        if (!bcKey.isPresent()) {
            return null;
        }
        return bcCommodityBought(bcKey.get());
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
                .setSpecification(bcSpec(key))
                .build());
    }

    @Nonnull
    private Collection<CommodityDTOs.CommoditySoldTO> commoditiesSoldList(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO) {
        // DSPMAccess and Datastore commodities are always dropped (shop-together or not)
        final boolean shopTogether = topologyDTO.getAnalysisSettings().getShopTogether();
        List<CommodityDTOs.CommoditySoldTO> list = topologyDTO.getCommoditySoldListList().stream()
            .filter(commSold -> commSold.getActive())
            .filter(commSold -> !isBicliqueCommodity(commSold.getCommodityType()))
            .filter(commSold -> includeGuaranteedBuyer
                || !AnalysisUtil.GUARANTEED_SELLER_TYPES.contains(topologyDTO.getEntityType())
                || !AnalysisUtil.VDC_COMMODITY_TYPES.contains(commSold.getCommodityType().getType()))
            .map(commSold -> commoditySold(commSold, topologyDTO))
            .collect(Collectors.toList());

        // In the case of non-shop-together, create the biclique commodities
        if (!shopTogether) {
            list.addAll(bcCommoditiesSold(topologyDTO));
        }
        return list;
    }

    /**
     * Create biclique commodity sold for entities. The commodity sold will play a role
     * in shop alone placement.
     *
     * @param topologyDTO the topologyDTO who should sell a biclique commodities
     * @return a set of biclique commodity sold DTOs
     */
    @Nonnull
    private Set<CommodityDTOs.CommoditySoldTO> bcCommoditiesSold(
                    @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO) {
        Set<String> bcKeys = dsBasedBicliquer.getBcKeys(String.valueOf(topologyDTO.getOid()));
        return bcKeys != null
                    ? bcKeys.stream()
                        .map(this::newBiCliqueCommoditySoldDTO)
                        .collect(Collectors.toSet())
                    : Collections.emptySet();
    }

    @Nonnull
    protected CommodityDTOs.CommoditySoldTO commoditySold(
                    @Nonnull final TopologyDTO.CommoditySoldDTO topologyCommSold,
                    TopologyDTO.TopologyEntityDTO dto) {
        final CommodityType commodityType = topologyCommSold.getCommodityType();
        float capacity = (float)topologyCommSold.getCapacity();
        float used = (float)topologyCommSold.getUsed();
        final int type = commodityType.getType();
        boolean resizable = topologyCommSold.getIsResizeable();
        boolean capacityNaN = Double.isNaN(topologyCommSold.getCapacity());
        boolean usedNaN = Double.isNaN(topologyCommSold.getUsed());
        if (capacityNaN && usedNaN) {
            resizable = true;
            logger.warn("Setting resizable for "
                            + commodityType + " to false");
        }
        if (used < 0) {
            if (logger.isDebugEnabled() || used != -1) {
                // We don't want to log every time we get used = -1 because mediation
                // sets some values to -1 as default.
                logger.warn("Setting negative used value for "
                                + commodityType + " to 0.");
            }
            used = 0;
        } else if (used > capacity) {
            if (AnalysisUtil.COMMODITIES_TO_CAP.contains(type)) {
                float cappedUsed = capacity * CAPACITY_FACTOR;
                logger.error("Used > Capacity for " + commodityType
                             + ". Used : " + used + ", Capacity : " + capacity
                             + ", Capped used : " + cappedUsed
                             + ". This is a mediation error and should be looked at.");
                used = cappedUsed;
            } else if (!(AnalysisUtil.COMMODITIES_TO_SKIP.contains(type) ||
                            AnalysisUtil.ACCESS_COMMODITY_TYPES.contains(type))) {
                logger.error("Used > Capacity for " + commodityType
                             + ". Used : " + used + " and Capacity : " + capacity);
            }
        }
        final CommodityDTOs.CommoditySoldSettingsTO economyCommSoldSettings =
                        CommodityDTOs.CommoditySoldSettingsTO.newBuilder()
                        .setResizable(resizable && !AnalysisUtil.PROVISIONED_COMMODITIES.contains(type))
                        .setCapacityIncrement(topologyCommSold.getCapacityIncrement())
                        .setCapacityUpperBound(capacity)
                        .setUtilizationUpperBound(
                            (float)(topologyCommSold.getEffectiveCapacityPercentage() / 100.0))
                        .setPriceFunction(priceFunction(topologyCommSold))
                        .setUpdateFunction(updateFunction(topologyCommSold))
                        .build();

        double maxQuantity = topologyCommSold.getMaxQuantity();
        float maxQuantityFloat = (float) maxQuantity;
        if (maxQuantity < 0) {
            logger.warn("maxQuantity: {} is less than 0. Setting it 0.", maxQuantity);
            maxQuantityFloat = 0;
        } else if (maxQuantityFloat < 0) {
            logger.warn("Float to double cast error. maxQuantity:{}. maxQuantityFloat:{}.",
                maxQuantity, maxQuantityFloat);
            maxQuantityFloat = 0;
        }
        // if entry not present, initialize to 0
        int numConsumers = Optional.ofNullable(numConsumersOfSoldCommTable.get(dto.getOid(),
                    topologyCommSold.getCommodityType())).map(o -> o.intValue()).orElse(0);
        return CommodityDTOs.CommoditySoldTO.newBuilder()
                        .setPeakQuantity((float)topologyCommSold.getPeak())
                        .setCapacity(capacity)
                        .setQuantity(used)
                        // Warning: we are down casting from double to float.
                        // Market has to change this field to double
                        .setMaxQuantity(maxQuantityFloat)
                        .setSettings(economyCommSoldSettings)
                        .setSpecification(commoditySpecification(commodityType))
                        .setThin(topologyCommSold.getIsThin())
                        .setNumConsumers(numConsumers)
                        .build();
    }

    private boolean isBicliqueCommodity(CommodityType commodityType) {
        boolean isOfType = AnalysisUtil.DSPM_OR_DATASTORE.contains(commodityType.getType());
        if (isOfType && commodityType.hasKey()) {
            // TODO: Support for cloud targets.
            // Exclude cloud targets imported from legacy from bicliques for the moment.
            // For non-cloud targets the key looks like
            // PhysicalMachine::7cd62bff-d6c8-e011-0000-00000000000f
            // or Storage::5787bc1e-357c82ea-47c4-0025b500038f.
            //
            // For cloud targets the key looks like PhysicalMachine::aws::us-west-2::PM::us-west-2b
            return StringUtils.countMatches(commodityType.getKey(), ":") < 4;
        }
        return false;
    }

    @Nonnull
    private CommodityDTOs.CommoditySoldTO newBiCliqueCommoditySoldDTO(String bcKey) {
        return CommodityDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(bcSpec(bcKey))
                        .setSettings(AnalysisUtil.BC_SETTING_TO)
                        .build();
    }

    @Nonnull
    private CommodityDTOs.CommoditySpecificationTO bcSpec(@Nonnull String bcKey) {
        return CommodityDTOs.CommoditySpecificationTO.newBuilder()
                        .setBaseType(bcBaseType())
                        .setType(commodityTypeAllocator.allocate(bcKey))
                        .setDebugInfoNeverUseInCode(BICLIQUE + " " + bcKey)
                        .build();
    }

    private int bcBaseType = -1;

    private int bcBaseType() {
        if (bcBaseType == -1) {
            bcBaseType = commodityTypeAllocator.allocate(BICLIQUE);
        }
        return bcBaseType;
    }

    @Nonnull
    private Optional<TopologyDTO.CommoditySoldDTO> commSoldTOtoCommSoldDTO(
        @Nonnull final CommodityDTOs.CommoditySoldTO commSoldTO) {
        return economyToTopologyCommodity(commSoldTO.getSpecification())
                .map(commType -> TopologyDTO.CommoditySoldDTO.newBuilder()
                    .setCapacity(commSoldTO.getCapacity())
                    .setPeak(commSoldTO.getPeakQuantity())
                    .setUsed(commSoldTO.getQuantity())
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

    @Nonnull
    private CommodityDTOs.CommoditySpecificationTO commoditySpecification(
            @Nonnull final CommodityType topologyCommodity) {
        final CommodityDTOs.CommoditySpecificationTO economyCommodity =
                        CommodityDTOs.CommoditySpecificationTO.newBuilder()
            .setType(toMarketCommodityId(topologyCommodity))
            .setBaseType(topologyCommodity.getType())
            .setDebugInfoNeverUseInCode(commodityDebugInfo(topologyCommodity))
            .setCloneWithNewType(AnalysisUtil.CLONE_COMMODITIES_WITH_NEW_TYPE
                    .contains(topologyCommodity.getType()))
            .build();
        commoditySpecMap.put(new EconomyCommodityId(economyCommodity), topologyCommodity);
        return economyCommodity;
    }

    /**
     * Select the right {@link PriceFunctionTO} based on the commodity sold type.
     *
     * @param topologyCommSold a commodity sold for which to add a price function
     * @return a (reusable) instance of PriceFunctionTO to use in the commodity sold settings.
     */
    @Nonnull
    private static PriceFunctionDTOs.PriceFunctionTO priceFunction(
            @Nonnull final TopologyDTO.CommoditySoldDTO topologyCommSold) {
        return AnalysisUtil.priceFunction(topologyCommSold.getCommodityType().getType());
    }

    /**
     * Select the right {@link UpdatingFunctionTO} based on the commodity sold type.
     *
     * @param topologyCommSold a commodity sold for which to add an updating function
     * @return a (reusable) instance of UpdatingFunctionTO to use in the commodity sold settings.
     */
    @Nonnull
    private static UpdatingFunctionTO updateFunction(
                    TopologyDTO.CommoditySoldDTO topologyCommSold) {
        return AnalysisUtil.updateFunction(topologyCommSold.getCommodityType().getType());
    }

    /**
     * Constructs a string that can be used for debug purposes.
     * @param commType the description of a commodity
     * @return a string in the format "VCPU|P1" when the specification includes a non-empty key
     * and just "VCPU" otherwise.
     */
    @Nonnull
    private static String commodityDebugInfo(
            @Nonnull final CommodityType commType) {
        final String key = commType.getKey();
        return CommodityDTO.CommodityType.forNumber(commType.getType())
                + (key == null || key.equals("") ? "" : ("|" + key));
    }

    /**
     * Uses a {@link NumericIDAllocator} to construct an integer type to
     * each unique combination of numeric commodity type + string key.
     * @param commType a commodity description that contains the numeric type and the key
     * @return and integer identifying the type
     */
    @VisibleForTesting
    int toMarketCommodityId(@Nonnull final CommodityType commType) {
        return commodityTypeAllocator.allocate(commodityTypeToString(commType));
    }

    @Nonnull
    private String commodityTypeToString(@Nonnull final CommodityType commType) {
        int type = commType.getType();
        return type + (commType.hasKey() ? COMMODITY_TYPE_KEY_SEPARATOR + commType.getKey() : "");
    }

    @VisibleForTesting
    @Nonnull
    CommodityType commodityIdToCommodityType(final int marketCommodityId) {
        return stringToCommodityType(commodityTypeAllocator.getName(marketCommodityId));
    }

    @Nonnull
    private CommodityType stringToCommodityType(@Nonnull final String commodityTypeString) {
        int separatorIndex = commodityTypeString.indexOf(COMMODITY_TYPE_KEY_SEPARATOR);
        if (separatorIndex > 0) {
            return CommodityType.newBuilder()
                .setType(Integer.parseInt(commodityTypeString.substring(0, separatorIndex)))
                .setKey(commodityTypeString.substring(separatorIndex + 1, commodityTypeString.length()))
                .build();
        } else {
            return CommodityType.newBuilder()
                .setType(Integer.parseInt(commodityTypeString))
                .build();
        }
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
     * @return The {@link Action} describing the recommendation in a topology-specific way.
     */
    @Nonnull
    public Optional<Action> interpretAction(@Nonnull final ActionTO actionTO,
                                            @Nonnull final Map<Long, Integer> entityIdToType) {
        try {
            // The action importance should never be infinite, as setImportance() will fail.
            final Action.Builder action = Action.newBuilder()
                // Assign a unique ID to each generated action.
                .setId(IdentityGenerator.next())
                            .setImportance(actionTO.getImportance())
                            .setExplanation(interpretExplanation(actionTO))
                .setExecutable(!actionTO.getIsNotExecutable());

            final ActionInfo.Builder infoBuilder = ActionInfo.newBuilder();

            switch (actionTO.getActionTypeCase()) {
                case MOVE:
                    infoBuilder.setMove(interpretMoveAction(actionTO.getMove(), entityIdToType));
                    break;
                case COMPOUND_MOVE:
                    infoBuilder.setMove(interpretCompoundMoveAction(actionTO.getCompoundMove(),
                            entityIdToType));
                    break;
                case RECONFIGURE:
                    infoBuilder.setReconfigure(interpretReconfigureAction(
                            actionTO.getReconfigure(), entityIdToType));
                    break;
                case PROVISION_BY_SUPPLY:
                    infoBuilder.setProvision(interpretProvisionBySupply(
                            actionTO.getProvisionBySupply(), entityIdToType));
                    break;
                case PROVISION_BY_DEMAND:
                    infoBuilder.setProvision(interpretProvisionByDemand(
                            actionTO.getProvisionByDemand(), entityIdToType));
                    break;
                case RESIZE:
                    infoBuilder.setResize(interpretResize(
                            actionTO.getResize(), entityIdToType));
                    break;
                case ACTIVATE:
                    infoBuilder.setActivate(interpretActivate(
                            actionTO.getActivate(), entityIdToType));
                    break;
                case DEACTIVATE:
                    infoBuilder.setDeactivate(interpretDeactivate(
                            actionTO.getDeactivate(), entityIdToType));
                    break;
                default:
                    return Optional.empty();
            }

            action.setInfo(infoBuilder);

            return Optional.of(action.build());
        } catch (RuntimeException e) {
            logger.error("Unable to interpret actionTO " + actionTO + " due to: ", e);
            return Optional.empty();
        }
    }

    private ActionDTO.Provision interpretProvisionByDemand(
                    @Nonnull final ProvisionByDemandTO provisionByDemandTO,
                    @Nonnull final Map<Long, Integer> entityIdToTypeMap) {
        return ActionDTO.Provision.newBuilder()
                .setEntityToClone(createActionEntity(
                    provisionByDemandTO.getModelSeller(), entityIdToTypeMap))
                .setProvisionedSeller(provisionByDemandTO.getProvisionedSeller())
                .build();
    }

    private Explanation interpretExplanation(ActionTO actionTO) {
        Explanation.Builder expBuilder = Explanation.newBuilder();
        switch (actionTO.getActionTypeCase()) {
            case MOVE:
                expBuilder.setMove(interpretMoveExplanation(actionTO.getMove()));
                break;
            case RECONFIGURE:
                expBuilder.setReconfigure(
                    interpretReconfigureExplanation(actionTO.getReconfigure()));
                break;
            case PROVISION_BY_SUPPLY:
                expBuilder.setProvision(
                    interpretProvisionExplanation(actionTO.getProvisionBySupply()));
                break;
            case PROVISION_BY_DEMAND:
                expBuilder.setProvision(
                    interpretProvisionExplanation(actionTO.getProvisionByDemand()));
                break;
            case RESIZE:
                expBuilder.setResize(
                    interpretResizeExplanation(actionTO.getResize()));
                break;
            case ACTIVATE:
                expBuilder.setActivate(
                    interpretActivateExplanation(actionTO.getActivate()));
                break;
            case DEACTIVATE:
                expBuilder.setDeactivate(
                    ActionDTO.Explanation.DeactivateExplanation.getDefaultInstance());
                break;
            case COMPOUND_MOVE:
                // TODO(COMPOUND): different moves in a compound move may have different explanations
                expBuilder.setMove(interpretCompoundMoveExplanation(actionTO.getCompoundMove().getMovesList()));
                break;
            default:
                throw new IllegalArgumentException("Market returned invalid action type "
                                + actionTO.getActionTypeCase());
        }
        return expBuilder.build();
    }

    private ActivateExplanation interpretActivateExplanation(ActivateTO activateTO) {
        return ActivateExplanation.newBuilder()
                .setMostExpensiveCommodity(activateTO.getMostExpensiveCommodity())
                .build();
    }

    private ResizeExplanation interpretResizeExplanation(ResizeTO resizeTO) {
        return ResizeExplanation.newBuilder().setStartUtilization(resizeTO.getStartUtilization())
                        .setEndUtilization(resizeTO.getEndUtilization()).build();
    }

    private ProvisionExplanation
                    interpretProvisionExplanation(ProvisionByDemandTO provisionByDemandTO) {
        final ShoppingListInfo shoppingList =
                        shoppingListOidToInfos.get(provisionByDemandTO.getModelBuyer());
        if (shoppingList == null) {
            throw new IllegalStateException(
                            "Market returned invalid shopping list for PROVISION_BY_DEMAND: "
                                            + provisionByDemandTO);
        } else {
            ProvisionExplanation.Builder expBuilder = ProvisionExplanation.newBuilder();
            List<ProvisionByDemandExplanation.CommodityNewCapacityEntry> capacityPerType =
                            new ArrayList<>();
            List<ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry> maxAmountPerType =
                            new ArrayList<>();
            provisionByDemandTO.getCommodityNewCapacityEntryList().forEach(newCapacityEntry -> {
                capacityPerType.add(
                    ActionDTO.Explanation.ProvisionExplanation
                        .ProvisionByDemandExplanation.CommodityNewCapacityEntry.newBuilder()
                    .setCommodityBaseType(newCapacityEntry.getCommodityBaseType())
                    .setNewCapacity(newCapacityEntry.getNewCapacity()).build());
            });
            provisionByDemandTO.getCommodityMaxAmountAvailableList().forEach(maxAmount -> {
                maxAmountPerType.add(
                    ActionDTO.Explanation.ProvisionExplanation
                        .ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry.newBuilder()
                    .setCommodityBaseType(maxAmount.getCommodityBaseType())
                    .setMaxAmountAvailable(maxAmount.getMaxAmountAvailable())
                    .setRequestedAmount(maxAmount.getRequestedAmount()).build());
            });
            return expBuilder.setProvisionByDemandExplanation(ProvisionByDemandExplanation
                    .newBuilder().setBuyerId(shoppingList.buyerId)
                    .addAllCommodityNewCapacityEntry(capacityPerType)
                    .addAllCommodityMaxAmountAvailable(maxAmountPerType).build()).build();
        }
    }

    private ProvisionExplanation
                    interpretProvisionExplanation(ProvisionBySupplyTO provisionBySupply) {
        return ProvisionExplanation.newBuilder()
                    .setProvisionBySupplyExplanation(
                            ProvisionBySupplyExplanation.newBuilder()
                                .setMostExpensiveCommodity(provisionBySupply
                                    .getMostExpensiveCommodity())
                                .build())
                    .build();
    }

    private ReconfigureExplanation
                    interpretReconfigureExplanation(ReconfigureTO reconfTO) {
        return ReconfigureExplanation.newBuilder()
                        .addAllReconfigureCommodity(reconfTO.getCommodityToReconfigureList().stream()
                            .map(this::commodityIdToCommodityType)
                            .collect(Collectors.toList()))
                        .build();
    }

    private MoveExplanation interpretCompoundMoveExplanation(List<MoveTO> moveTOs) {
        MoveExplanation.Builder moveExpBuilder = MoveExplanation.newBuilder();
        moveTOs.stream()
            .map(MoveTO::getMoveExplanation)
            .map(this::changeExplanation)
            .forEach(moveExpBuilder::addChangeProviderExplanation);
        return moveExpBuilder.build();
    }

    private MoveExplanation interpretMoveExplanation(MoveTO moveTO) {
        MoveExplanation.Builder moveExpBuilder = MoveExplanation.newBuilder();
        moveExpBuilder.addChangeProviderExplanation(changeExplanation(moveTO.getMoveExplanation()));
        return moveExpBuilder.build();
    }

    private ChangeProviderExplanation changeExplanation(
            com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation moveExplanation) {
        switch (moveExplanation.getExplanationTypeCase()) {
            case COMPLIANCE:
                return ChangeProviderExplanation.newBuilder()
                        .setCompliance(ChangeProviderExplanation.Compliance.newBuilder()
                            .addAllMissingCommodities(
                                moveExplanation.getCompliance()
                                    .getMissingCommoditiesList().stream()
                                    .map(this::commodityIdToCommodityType)
                                    .collect(Collectors.toList())
                            )
                            .build())
                        .build();
            case CONGESTION:
                return ChangeProviderExplanation.newBuilder()
                        .setCongestion(ChangeProviderExplanation.Congestion.newBuilder()
                            .addAllCongestedCommodities(
                                moveExplanation.getCongestion().getCongestedCommoditiesList().stream()
                                    .map(this::commodityIdToCommodityType)
                                    .collect(Collectors.toList()))
                            .build())
                        .build();
            case EVACUATION:
                return ChangeProviderExplanation.newBuilder()
                        .setEvacuation(ChangeProviderExplanation.Evacuation.newBuilder()
                            .setSuspendedEntity(moveExplanation.getEvacuation().getSuspendedTrader())
                            .build())
                        .build();
            case INITIALPLACEMENT:
                return ChangeProviderExplanation.newBuilder()
                        .setInitialPlacement(ChangeProviderExplanation.InitialPlacement.getDefaultInstance())
                    .build();
            case PERFORMANCE:
                return ChangeProviderExplanation.newBuilder()
                        .setPerformance(ChangeProviderExplanation.Performance.getDefaultInstance())
                    .build();
            default:
                logger.error("Unknown explanation case for move action: "
                    + moveExplanation.getExplanationTypeCase());
                return ChangeProviderExplanation.getDefaultInstance();
        }
    }

    @Nonnull
    private ActionDTO.Move interpretMoveAction(@Nonnull final MoveTO moveTO,
                                       @Nonnull final Map<Long, Integer> entityIdToEntityType) {
        final ShoppingListInfo shoppingList =
                        shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        if (shoppingList == null) {
            throw new IllegalStateException(
                            "Market returned invalid shopping list for MOVE: " + moveTO);
        } else {
            return ActionDTO.Move.newBuilder()
                            .setTarget(
                                    createActionEntity(shoppingList.buyerId, entityIdToEntityType))
                            .addChanges(createChangeProvider(moveTO, entityIdToEntityType))
                            .build();
        }
    }

    @Nonnull
    private ActionDTO.Move interpretCompoundMoveAction(
                    @Nonnull final CompoundMoveTO compoundMoveTO,
                    @Nonnull final Map<Long, Integer> entityIdToEntityType) {
        List<MoveTO> moves = compoundMoveTO.getMovesList();
        if (moves.isEmpty()) {
            throw new IllegalStateException(
                "Market returned no moves in a COMPOUND_MOVE: " + compoundMoveTO);
        }
        Set<Long> targetIds = moves.stream()
                        .map(MoveTO::getShoppingListToMove)
                        .map(shoppingListOidToInfos::get)
                        .map(ShoppingListInfo::getBuyerId)
                        .collect(Collectors.toSet());
        if (targetIds.size() != 1) {
            throw new IllegalStateException(
                (targetIds.isEmpty() ? "Empty target ID" : "Non-unique target IDs")
                    + " in COMPOUND_MOVE:" + compoundMoveTO);
        }

        return ActionDTO.Move.newBuilder()
                        .setTarget(createActionEntity(
                            targetIds.iterator().next(), entityIdToEntityType))
                        .addAllChanges(moves.stream()
                            .map(move -> createChangeProvider(move, entityIdToEntityType))
                            .collect(Collectors.toList()))
                            .build();
    }

    @Nonnull
    private ChangeProvider createChangeProvider(@Nonnull final MoveTO move,
                                        @Nonnull final Map<Long, Integer> entityIdToEntityType) {
        // move action could have no source id, for example: initial placement action.
        Preconditions.checkArgument(
            !move.hasSource() || entityIdToEntityType.containsKey(move.getSource()),
            "Missing entityType in the map for source entity %s", move.getSource());

        Preconditions.checkArgument(entityIdToEntityType.containsKey(move.getDestination()),
            "Missing entityType in the map for destination entity %s", move.getDestination());

        final ChangeProvider.Builder changeProviderBuilder = ChangeProvider.newBuilder()
                    .setDestination(ActionEntity.newBuilder()
                        .setId(move.getDestination())
                        .setType(entityIdToEntityType.get(move.getDestination())));
        if (move.hasSource()) {
            changeProviderBuilder.setSource(ActionEntity.newBuilder()
                    .setId(move.getSource())
                    .setType(entityIdToEntityType.get(move.getSource())));
        }
        return changeProviderBuilder.build();
    }

    @Nonnull
    private ActionDTO.Reconfigure interpretReconfigureAction(
                    @Nonnull final ReconfigureTO reconfigureTO,
                    @Nonnull final Map<Long, Integer> entityIdToEntityType) {
        final ShoppingListInfo shoppingList =
                        shoppingListOidToInfos.get(reconfigureTO.getShoppingListToReconfigure());
        if (shoppingList == null) {
            throw new IllegalStateException(
                "Market returned invalid shopping list for RECONFIGURE: " + reconfigureTO);
        } else {
            final ActionDTO.Reconfigure.Builder builder = ActionDTO.Reconfigure.newBuilder()
                .setTarget(createActionEntity(shoppingList.buyerId, entityIdToEntityType));

            if (reconfigureTO.hasSource()) {
                builder.setSource(createActionEntity(reconfigureTO.getSource(), entityIdToEntityType));
            }

            return builder.build();
        }
    }

    @Nonnull
    private ActionDTO.Provision interpretProvisionBySupply(
                    @Nonnull final ProvisionBySupplyTO provisionBySupplyTO,
                    @Nonnull final Map<Long, Integer> entityIdToEntityType) {
        return ActionDTO.Provision.newBuilder()
                .setEntityToClone(createActionEntity(
                    provisionBySupplyTO.getModelSeller(), entityIdToEntityType))
                .setProvisionedSeller(provisionBySupplyTO.getProvisionedSeller())
                .build();
    }

    @Nonnull
    private ActionDTO.Resize interpretResize(@Nonnull final ResizeTO resizeTO,
                                         @Nonnull final Map<Long, Integer> entityIdToEntityType) {
        final long entityId = resizeTO.getSellingTrader();
        final CommodityType topologyCommodity =
                economyToTopologyCommodity(resizeTO.getSpecification())
                    .orElseThrow(() -> new IllegalArgumentException(
                        "Resize commodity can't be converted to topology commodity format! "
                            + resizeTO.getSpecification()));
        // Determine if this is a remove limit or a regular resize.
        if (entityIdToEntityType.containsKey(entityId)) {
            if (EntityType.VIRTUAL_MACHINE.getNumber() == entityIdToEntityType.get(entityId)) {
                // If this is a VM and has a restricted capacity, we are going to assume it's a limit
                // removal. This logic seems like it could be fragile, in that limit may not be the
                // only way VM capacity could be restricted in the future, but this is consistent
                // with how classic makes the same decision.
                TraderTO traderTO = oidToTraderTOMap.get(entityId);
                // find the commodity on the trader and see if there is a limit?
                for (CommoditySoldTO commoditySold : traderTO.getCommoditiesSoldList()) {
                    if (commoditySold.getSpecification().equals(resizeTO.getSpecification())) {
                        // We found the commodity sold.  If it has a utilization upper bound < 1.0,
                        // then the commodity is restricted, and according to our VM-rule, we will
                        // treat this as a limit removal.
                        float utilizationPercentage = commoditySold.getSettings().getUtilizationUpperBound();
                        if (utilizationPercentage < 1.0) {
                            // The "limit removal" is actually a resize on the commodity's "limit"
                            // attribute that effectively sets it to zero.
                            //
                            // Ideally we would set the "old capacity" to the current limit
                            // value, but as noted above, we don't have access to the limit here. We
                            // only have the utilization percentage, which we expect to be based on
                            // the limit and raw capacity values. Since we do have the utilization %
                            // and raw capacity here, we can _approximate_ the current limit by
                            // reversing the math used to determine the utilization %.
                            //
                            // We will grudgingly do that here. Note that this may be subject to
                            // precision and rounding errors. In addition, if in the future we have
                            // factors other than "limit" that could drive the VM resource
                            // utilization threshold to below 100%, then this approximation would
                            // likely be wrong and misleading in those cases.
                            float approximateLimit = commoditySold.getCapacity() * utilizationPercentage;
                            logger.debug("The commodity {} has util% of {}, so treating as limit"
                                    +" removal (approximate limit: {}).",
                                    topologyCommodity.getKey(), utilizationPercentage, approximateLimit);

                            return ActionDTO.Resize.newBuilder()
                                    .setTarget(createActionEntity(entityId, entityIdToEntityType))
                                    .setOldCapacity(approximateLimit)
                                    .setNewCapacity(0)
                                    .setCommodityType(topologyCommodity)
                                    .setCommodityAtribute(CommodityAttribute.LIMIT)
                                    .build();
                        }
                        break;
                    }
                }
            }
        }
        return ActionDTO.Resize.newBuilder()
                .setTarget(createActionEntity(entityId, entityIdToEntityType))
                .setNewCapacity(resizeTO.getNewCapacity())
                .setOldCapacity(resizeTO.getOldCapacity())
                .setCommodityType(topologyCommodity)
                .build();
    }

    @Nonnull
    private ActionDTO.Activate interpretActivate(@Nonnull final ActivateTO activateTO,
                                     @Nonnull final Map<Long, Integer> entityIdToEntityType) {
        final long entityId = activateTO.getTraderToActivate();
        final List<CommodityType> topologyCommodities =
                activateTO.getTriggeringBasketList().stream()
                        .map(this::economyToTopologyCommodity)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());
        return ActionDTO.Activate.newBuilder()
                .setTarget(createActionEntity(entityId, entityIdToEntityType))
                .addAllTriggeringCommodities(topologyCommodities)
                .build();
    }

    @Nonnull
    private ActionDTO.Deactivate interpretDeactivate(@Nonnull final DeactivateTO deactivateTO,
                                     @Nonnull final Map<Long, Integer> entityIdToEntityType) {
        final long entityId = deactivateTO.getTraderToDeactivate();
        final List<CommodityType> topologyCommodities =
                deactivateTO.getTriggeringBasketList().stream()
                        .map(this::economyToTopologyCommodity)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());
        return ActionDTO.Deactivate.newBuilder()
                .setTarget(createActionEntity(entityId, entityIdToEntityType))
                .addAllTriggeringCommodities(topologyCommodities)
                .build();
    }

    @VisibleForTesting
    @Nonnull
    Optional<CommodityType> economyToTopologyCommodity(
            @Nonnull final CommodityDTOs.CommoditySpecificationTO economyCommodity) {
        final CommodityType topologyCommodity =
                commoditySpecMap.get(new EconomyCommodityId(economyCommodity));
        if (topologyCommodity == null) {
            if (commodityTypeAllocator.getName(economyCommodity.getBaseType()).equals(BICLIQUE)) {
                // this is a biclique commodity
                return Optional.empty();
            }
            throw new IllegalStateException("Market returned invalid commodity specification " +
                    economyCommodity + "! " +
                    "Registered ones are " + commoditySpecMap.keySet());
        }
        return Optional.of(topologyCommodity);
    }

    /**
     * The {@link CommodityDTOs.CommoditySpecificationTO}s we get back from the market aren't exactly
     * equal to the ones we pass in - for example, the debug info may be absent. We only want to
     * compare the type and base_type.
     */
    private static class EconomyCommodityId {
        final int type;
        final int baseType;

        EconomyCommodityId(@Nonnull final CommodityDTOs.CommoditySpecificationTO economyCommodity) {
            this.type = economyCommodity.getType();
            this.baseType = economyCommodity.getBaseType();
        }

        @Override
        public int hashCode() {
            return type & baseType;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof EconomyCommodityId) {
                final EconomyCommodityId otherId = (EconomyCommodityId)other;
                return otherId.type == type && otherId.baseType == baseType;
            } else {
                return false;
            }
        }
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
        return;
    }

    private ActionEntity createActionEntity(final long id,
                                @Nonnull final Map<Long, Integer> entityIdToEntityType) {
        return ActionEntity.newBuilder()
                    .setId(id)
                    .setType(entityIdToEntityType.get(id))
                    .build();
    }
}
