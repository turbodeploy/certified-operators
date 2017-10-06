package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults.NewShoppingListToBuyerEntry;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Convert topology DTOs to economy DTOs.
 */
public class TopologyConverter {

    /**
     * A non-shop-together TopologyConverter.
     */
    public TopologyConverter() {
    }

    private final Logger logger = LogManager.getLogger();

    private static final String BICLIQUE = "BICLIQUE";

    public static final Set<TopologyDTO.EntityState> SKIPPED_ENTITY_STATES = ImmutableSet.of(
        TopologyDTO.EntityState.UNKNOWN, TopologyDTO.EntityState.MAINTENANCE);

    // TODO: In legacy this is taken from LicenseManager and is currently false
    private boolean includeGuaranteedBuyer = false;

    /**
     * Map from entity OID to entity.
     */
    private Map<Long, TopologyDTO.TopologyEntityDTO> entityOidToDto = Maps.newHashMap();

    // a map keeps shoppinglist oid to ShoppingListInfo which is a container for
    // shoppinglist oid, buyer oid, seller oid and commodity bought
    private final Map<Long, ShoppingListInfo> shoppingListOidToInfos = Maps.newHashMap();

    private final Map<EconomyCommodityId, CommodityType>
            commoditySpecMap = Maps.newHashMap();

    private long shoppingListId = 1000L; // Arbitrary start value

    private AtomicLong cloneIndex = new AtomicLong(0);

    private final NumericIDAllocator commodityTypeAllocator = new NumericIDAllocator();

    // a list of oids of guaranteed buyers, used to skip creating shopping lists that buy from
    // these entities (acting as sellers) when includeGuaranteedBuyer is false.
    private @Nonnull List<Long> guaranteedList = new ArrayList<Long>();

 // a map to keep the oid to traderTO mapping, it also includes newly cloned traderTO
    private Map<Long, EconomyDTOs.TraderTO> oidToTraderTOMap = Maps.newHashMap();

    /**
     * Constructor with includeGuaranteedBuyer parameter.
     *
     * @param includeGuaranteedBuyer whether to include guaranteed buyers (VDC, VPod, DPod) or not
     */
    public TopologyConverter(boolean includeGuaranteedBuyer) {
        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
    }

    // Biclique stuff
    private final BiCliquer bicliquer = new BiCliquer();

    // Map from bcKey to commodity bought
    private Map<String, EconomyDTOs.CommodityBoughtTO> bcCommodityBoughtMap = Maps.newHashMap();
    // a BiMap from DSPMAccess and Datastore commodity sold key to seller oid
    // Note: the commodity key is composed of entity type and entity ID (which is different from
    // OID)
    private BiMap<String, Long> accessesByKey = HashBiMap.create();
    // When shopTogether is enabled, we don't create biclique commodities. Instead we add the
    // biclique IDs to the allCliques property of each trader that is a member of bicliques.
    // In both cases (shop-together and non-shop-together) we eliminate the DSPMAccess commodities
    // and datastore commodities (both bought and sold).
    private boolean shopTogether = false;

    private Set<EconomyDTOs.CommoditySoldTO> EMPTY_SET = Sets.newHashSet();
    private Set<Long> EMPTY_LONG_SET = Sets.newHashSet();

    /**
     * A shop-together TopologyConverter.
     *
     * @return an instance of TopologyConverter that applies shop-together biclique creation
     */
    public static TopologyConverter shopTogetherConverter() {
        TopologyConverter converter = new TopologyConverter(false);
        converter.shopTogether = true;
        return converter;
    }

    /**
     * Convert a list of common protobuf topology entity DTOs to analysis protobuf economy DTOs.
     * @param entities list of topology entity DTOs
     * @return set of economy DTOs
     * @throws InvalidTopologyException when the topology is invalid, e.g. used > capacity
     */
    @Nonnull
    public Set<EconomyDTOs.TraderTO> convertToMarket(
                @Nonnull final Collection<TopologyDTO.TopologyEntityDTO> entities)
                                throws InvalidTopologyException {
        for (TopologyDTO.TopologyEntityDTO entity : entities) {
            int entityType = entity.getEntityType();
            if (AnalysisUtil.SKIPPED_ENTITY_TYPES.contains(entityType)
                || SKIPPED_ENTITY_STATES.contains(entity.getEntityState())) {
                continue;
            } else if (!includeGuaranteedBuyer && AnalysisUtil.GUARANTEED_BUYER_TYPES.contains(entityType)) {
                guaranteedList.add(entity.getOid());
            } else {
                entityOidToDto.put(entity.getOid(), entity);
            }
        }
        return convertToMarket();
    }

    /**
     * Convert a map of common protobuf topology entity DTOs to analysis protobuf economy DTOs.
     * @return set of economy DTOs
     * @throws InvalidTopologyException when the topology is invalid, e.g. used > capacity
     */
    @Nonnull
    private Set<EconomyDTOs.TraderTO> convertToMarket() throws InvalidTopologyException {
        logger.info("Converting topologyEntityDTOs to traderTOs");
        logger.debug("Start creating bicliques");

        BiMap<Long, String> oidToUuidMap = HashBiMap.create();
        for (TopologyDTO.TopologyEntityDTO dto : entityOidToDto.values()) {
            dto.getCommoditySoldListList().stream()
                .filter(comm -> isBicliqueCommodity(comm.getCommodityType()))
                .forEach(comm -> edge(dto, comm));
            oidToUuidMap.put(dto.getOid(), String.valueOf(dto.getOid()));
        }
        bicliquer.compute(oidToUuidMap);
        logger.debug("Done creating bicliques");
        List<String> exceptions = Lists.newArrayList();
        final ImmutableSet.Builder<EconomyDTOs.TraderTO> returnBuilder = ImmutableSet.builder();
        entityOidToDto.values().stream()
                .map(dto -> {
                    try {
                        return topologyDTOtoTraderTO(dto);
                    } catch (InvalidTopologyException e) {
                        exceptions.add(e.getMessage());
                    }
                    return null;
                })
                .filter(trader -> trader != null)
                .forEach(returnBuilder::add);
        if (!exceptions.isEmpty()) {
            throw new InvalidTopologyException(
                "Invalid entities :\n" + exceptions.stream().collect(Collectors.joining("\n")));
        }
        logger.info("Converted topologyEntityDTOs to traderTOs");
        return returnBuilder.build();
    }

    private void edge(TopologyDTO.TopologyEntityDTO dto, TopologyDTO.CommoditySoldDTO commSold) {
        accessesByKey.computeIfAbsent(commSold.getCommodityType().getKey(),
            key -> commSold.getAccesses());
        if (commSold.getCommodityType().getType() == CommodityDTO.CommodityType.DSPM_ACCESS_VALUE) {
            // Storage id first, PM id second.
            // This way each storage is a member of exactly one biclique.
            bicliquer.edge(String.valueOf(dto.getOid()), String.valueOf(commSold.getAccesses()));
        }
    }

    /**
     * Convert the {@link EconomyDTOs.TraderTO}s to {@link TopologyDTO.TopologyEntityDTO}s. This method
     * creates a lazy collection, which is really converted during iteration.
     *
     * @param traderTOs list of {@link EconomyDTOs.TraderTO}s that are to be converted to
     * {@link TopologyDTO.TopologyEntityDTO}s
     * @param entityByOid whose key is the traderOid and the value is the original traderTO
     * @return list of {@link TopologyDTO.TopologyEntityDTO}s
     */
    @Nonnull
    private List<TopologyDTO.TopologyEntityDTO> convertFromMarket(
                 @Nonnull final List<EconomyDTOs.TraderTO> traderTOs,
                 @Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> entityByOid) {
        logger.info("Converting traderTOs to topologyEntityDTOs");
        oidToTraderTOMap = traderTOs.stream().collect(
                        Collectors.toMap(EconomyDTOs.TraderTO::getOid, Function.identity()));
        // Perform lazy transformation, so do not store all the TopologyEntityDTOs in memory
        return Lists.transform(traderTOs, dto -> traderTOtoTopologyDTO(dto, entityByOid));
    }

    /**
     * Convert the {@link EconomyDTOs.TraderTO}s to {@link TopologyDTO.TopologyEntityDTO}s. This method
     * creates a lazy collection, which is really converted during iteration.
     *
     * @param traderTOs list of {@link EconomyDTOs.TraderTO}s that are to be converted to
     * {@link TopologyDTO.TopologyEntityDTO}s
     * @param topologyDTOs the original set of {@link TopologyDTO.TopologyEntityDTO}s
     * @return list of {@link TopologyDTO.TopologyEntityDTO}s
     */
    @Nonnull
    public List<TopologyDTO.TopologyEntityDTO> convertFromMarket(
                    @Nonnull final List<EconomyDTOs.TraderTO> traderTOs,
                    @Nonnull final Set<TopologyDTO.TopologyEntityDTO> topologyDTOs) {
        Map<Long, TopologyDTO.TopologyEntityDTO> entityByOid =
                        getEntityMap(topologyDTOs);
        return convertFromMarket(traderTOs, entityByOid);
    }

    /**
     * Create a map which is indexed by the traderOid and the value is the
     * corresponding {@link TopologyDTO.TopologyEntityDTO}.
     *
     * @param topologyDTOs list of {@link TopologyDTO.TopologyEntityDTO}s
     * @return Map whose key is the traderOid and the value is the original
     * {@link TopologyDTO.TopologyEntityDTO}
     */
    private static Map<Long, TopologyDTO.TopologyEntityDTO> getEntityMap(
                   @Nonnull final Set<TopologyDTO.TopologyEntityDTO> topologyDTOs) {

        return topologyDTOs.stream()
                .collect(Collectors.toMap(
                    TopologyDTO.TopologyEntityDTO::getOid, Function.identity()));
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
        Map<Long, TopologyDTO.TopologyEntityDTO.CommodityBoughtList> topoDTOCommBoughtMap =
                        Maps.newHashMap();
        long pmOid = 0L;
        List<Long> storageOidList = new ArrayList<>();
        // for a VM, find its associated PM to ST list map
        if (traderTO.getType() == EntityType.VIRTUAL_MACHINE_VALUE) {
            Map<Long, List<Long>> pm2stMap = createPMToSTMap(traderTO.getShoppingListsList());
            // there should be only one pm supplier for vm
            pmOid = pm2stMap.keySet().iterator().next();
            storageOidList = pm2stMap.get(pmOid);
        }
        for (EconomyDTOs.ShoppingListTO sl : traderTO.getShoppingListsList()) {
            List<TopologyDTO.CommodityBoughtDTO> commList =
                            new ArrayList<TopologyDTO.CommodityBoughtDTO>();
            int bicliqueBaseType = commodityTypeAllocator.allocate(BICLIQUE);
            for (EconomyDTOs.CommodityBoughtTO commBought : sl.getCommoditiesBoughtList()) {
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
            topoDTOCommBoughtMap.put(sl.getSupplier(), TopologyDTO.TopologyEntityDTO
                .CommodityBoughtList.newBuilder().addAllCommodityBought(commList).build());
        }

        TopologyDTO.EntityState entityState = TopologyDTO.EntityState.POWERED_ON;

        TopologyDTO.TopologyEntityDTO originalTrader = traderOidToEntityDTO.get(traderTO.getOid());
        String displayName = originalTrader != null ? originalTrader.getDisplayName()
                        : traderOidToEntityDTO.get(traderTO.getCloneOf()).getDisplayName()
                        + "_Clone #" + cloneIndex.addAndGet(1);
        if (originalTrader != null) {
            // set state of IDLE VM to poweredOff
            entityState = originalTrader.getEntityState() == TopologyDTO.EntityState.POWERED_OFF
                            ? TopologyDTO.EntityState.POWERED_OFF
                            : TopologyDTO.EntityState.POWERED_ON;
        }
        if (entityState == TopologyDTO.EntityState.POWERED_ON) {
            entityState = (traderTO.getState() == EconomyDTOs.TraderStateTO.ACTIVE)
                            ? TopologyDTO.EntityState.POWERED_ON
                            : TopologyDTO.EntityState.SUSPENDED;
        }

        TopologyDTO.TopologyEntityDTO.ProviderPolicy policy =
                TopologyDTO.TopologyEntityDTO.ProviderPolicy.newBuilder()
                    .setIsAvailableAsProvider(traderTO.getSettings().getCanAcceptNewCustomers())
                    .build();
        TopologyDTO.TopologyEntityDTO.Builder entityDTO =
                TopologyDTO.TopologyEntityDTO.newBuilder()
                    .setEntityType(traderTO.getType())
                    .setOid(traderTO.getOid())
                    .setEntityState(entityState)
                    .setDisplayName(displayName)
                    .addAllCommoditySoldList(retrieveCommSoldList(traderTO))
                    .putAllCommodityBoughtMap(topoDTOCommBoughtMap)
                    .setProviderPolicy(policy);
        if (originalTrader == null) {
            // this is a clone trader
            originalTrader = traderOidToEntityDTO.get(traderTO.getCloneOf());
        }
        // get dspm and datastore commodity sold from the original trader, add
        // them to projected topology entity DTO
        return entityDTO.addAllCommoditySoldList(originalTrader.getCommoditySoldListList().stream()
                        .filter(c -> AnalysisUtil.DSPM_OR_DATASTORE
                                        .contains(c.getCommodityType().getType()))
                        .collect(Collectors.toSet()))
                        .build();
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
     * Convert {@link EconomyDTOs.CommodityBoughtTO} of a trader to its corresponding
     * {@link TopologyDTO.CommodityBoughtDTO}.
     *
     * @param commBoughtTO {@link EconomyDTOs.CommodityBoughtTO} that is to be converted to
     * {@link TopologyDTO.CommodityBoughtDTO}
     * @return {@link TopologyDTO.CommoditySoldDTO} that the trader sells
     */
    @Nonnull
    private Optional<TopologyDTO.CommodityBoughtDTO> commBoughtTOtoCommBoughtDTO(
            @Nonnull final EconomyDTOs.CommodityBoughtTO commBoughtTO) {
        return economyToTopologyCommodity(commBoughtTO.getSpecification())
                .map(commType -> TopologyDTO.CommodityBoughtDTO.newBuilder()
                        .setUsed(commBoughtTO.getQuantity())
                        .setCommodityType(commType)
                        .setPeak(commBoughtTO.getPeakQuantity())
                        .build());
    }

    private EconomyDTOs.TraderTO topologyDTOtoTraderTO(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO)
                            throws InvalidTopologyException {
        final EconomyDTOs.TraderStateTO state = traderState(topologyDTO.getEntityState());
        final boolean active = EconomyDTOs.TraderStateTO.ACTIVE.equals(state);
        final boolean bottomOfSupplyChain = topologyDTO.getCommodityBoughtMapMap().isEmpty();
        final int entityType = topologyDTO.getEntityType();
        boolean clonable = AnalysisUtil.CLONABLE_TYPES.contains(entityType); // TODO: should use settings
        boolean suspendable = true; // TODO: should use settings
        if (bottomOfSupplyChain && active) {
            clonable = AnalysisUtil.CLONABLE_TYPES.contains(entityType);
            suspendable = false;
        }
        final boolean isGuranteedBuyer = guaranteedBuyer(topologyDTO);
        final EconomyDTOs.TraderSettingsTO settings = EconomyDTOs.TraderSettingsTO.newBuilder()
            .setClonable(clonable)
            .setSuspendable(suspendable)
            .setMinDesiredUtilization(0.7f) // TODO: should use settings
            .setMaxDesiredUtilization(0.8f)  // TODO: should use settings
            .setGuaranteedBuyer(isGuranteedBuyer)
            .setCanAcceptNewCustomers(topologyDTO.getProviderPolicy().getIsAvailableAsProvider())
            .build();
        Set<Long> allCliques = shopTogether
                        ? bicliquer.getBcIDs(String.valueOf(topologyDTO.getOid()))
                        : EMPTY_LONG_SET;
        if (allCliques == null) {
            allCliques = EMPTY_LONG_SET;
        }

        return EconomyDTOs.TraderTO.newBuilder()
            // Type and Oid are the same in the topology DTOs and economy DTOs
            .setOid(topologyDTO.getOid())
            .setType(entityType)
            .setState(state)
            .setSettings(settings)
            .setDebugInfoNeverUseInCode(entityDebugInfo(topologyDTO))
            .addAllCommoditiesSold(commoditiesSoldList(topologyDTO))
            .addAllShoppingLists(createAllShoppingLists(topologyDTO))
            .addAllCliques(allCliques)
            .build();
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
                        && topologyDTO.getCommodityBoughtMapMap().keySet().stream()
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
            @Nonnull final TopologyDTO.EntityState topologyEntityState) {
        return topologyEntityState == TopologyDTO.EntityState.POWERED_ON
                ? EconomyDTOs.TraderStateTO.ACTIVE
                : EconomyDTOs.TraderStateTO.INACTIVE;
    }

    @Nonnull
    private List<EconomyDTOs.ShoppingListTO> createAllShoppingLists(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyEntity) {
        return topologyEntity.getCommodityBoughtMapMap().entrySet().stream()
            // skip converting shoppinglist that buys from VDC
            .filter(commBoughtMap -> !guaranteedList.contains(commBoughtMap.getKey()))
            .map(commBoughtMap -> createShoppingList(
                    topologyEntity.getOid(),
                    topologyEntity.getEntityType(),
                    commBoughtMap.getKey(),
                    commBoughtMap.getValue()))
            .collect(Collectors.toList());
    }

    /**
     * Create a shopping list for a specified buyer and the entity it is buying from.
     * @param buyerOid the OID of the buyer of the shopping list
     * @param entityType the entity type of the buyer
     * @param providerOid the oid of the seller of the shopping list
     * @param commoditiesBought the commodities bought by the buyer from the provider
     * @return a shopping list between the buyer and seller
     */
    @Nonnull
    private EconomyDTOs.ShoppingListTO createShoppingList(
            final long buyerOid,
            final long entityType,
            final long providerOid,
            @Nonnull final TopologyDTO.TopologyEntityDTO.CommodityBoughtList commoditiesBought) {
        TopologyDTO.TopologyEntityDTO provider = entityOidToDto.get(providerOid);
        float moveCost = (entityType == EntityType.VIRTUAL_MACHINE_VALUE
                && provider != null // this check is for testing purposes
                && provider.getEntityType() == EntityType.STORAGE_VALUE)
                        ? (float)(totalStorageAmountBought(buyerOid) / Units.KIBI)
                        : 0.0f;
        Set<EconomyDTOs.CommodityBoughtTO> values = commoditiesBought.getCommodityBoughtList()
            .stream()
            .filter(topoCommBought -> topoCommBought.getActive())
            .map(topoCommBought -> convertCommodityBought(topoCommBought, providerOid))
            .filter(comm -> comm != null) // Null for DSPMAccess/Datastore and shop-together
            .collect(Collectors.toSet());
        final long id = shoppingListId++;
        final EconomyDTOs.ShoppingListTO economyShoppingList = EconomyDTOs.ShoppingListTO
                .newBuilder()
                .addAllCommoditiesBought(values)
                .setSupplier(providerOid)
                .setOid(id)
                .setStorageMoveCost(moveCost)
                .setMovable(AnalysisUtil.MOVABLE_TYPES.contains((int)entityType))
                .build();
        shoppingListOidToInfos.put(id,
            new ShoppingListInfo(id, buyerOid, providerOid, commoditiesBought));
        return economyShoppingList;
    }

    /**
     * The total used value of storage amount commodities bought by a buyer,
     * summed over all the providers that it buys storage amount from.
     *
     * @param buyerOid the OID of the buyer (presumably a VM)
     * @return total used storage amount bought
     */
    private double totalStorageAmountBought(long buyerOid) {
        return entityOidToDto.get(buyerOid).getCommodityBoughtMapMap().entrySet().stream()
            .filter(this::isStorage)
            .map(Map.Entry::getValue)
            .map(TopologyDTO.TopologyEntityDTO.CommodityBoughtList::getCommodityBoughtList)
            .flatMap(List::stream)
            .filter(this::isStorageAmount)
            .mapToDouble(TopologyDTO.CommodityBoughtDTO::getUsed)
            .sum();
    }

    /**
     * Checks whether the entry's key is the OID of a Storage.
     *
     * @param entry a Map entry where the key is an oid
     * @return whether the entry's key is the oid of a Storage
     */
    private boolean isStorage(Map.Entry<Long, ?> entry) {
        TopologyDTO.TopologyEntityDTO entity = entityOidToDto.get(entry.getKey());
        return entity != null && entity.getEntityType() == EntityType.STORAGE_VALUE;
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

    @Nullable
    private EconomyDTOs.CommodityBoughtTO convertCommodityBought(
            @Nonnull final TopologyDTO.CommodityBoughtDTO topologyCommBought, long providerOid) {
        CommodityType type = topologyCommBought.getCommodityType();
        return isBicliqueCommodity(type)
            ? shopTogether
                // skip DSPMAcess and Datastore commodities in the case of shop-together
                ? null
                // convert them to biclique commodities if not shop-together
                : bcCommodityBought(bicliquer.getBcKey(
                    String.valueOf(providerOid),
                    String.valueOf(accessesByKey.get(type.getKey()))
                    ))
            // all other commodities - convert to DTO regardless of shop-together
            : EconomyDTOs.CommodityBoughtTO.newBuilder()
                .setQuantity((float)topologyCommBought.getUsed())
                .setPeakQuantity((float)topologyCommBought.getPeak())
                .setSpecification(commoditySpecification(topologyCommBought.getCommodityType()))
                .build();
    }

    @Nonnull
    private EconomyDTOs.CommodityBoughtTO bcCommodityBought(String bcKey) {
        return bcCommodityBoughtMap.computeIfAbsent(bcKey,
            key -> EconomyDTOs.CommodityBoughtTO.newBuilder()
                .setSpecification(bcSpec(key))
                .build());
    }

    @Nonnull
    private Collection<EconomyDTOs.CommoditySoldTO> commoditiesSoldList(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO)
                            throws InvalidTopologyException {
        // DSPMAccess and Datastore commodities are always dropped (shop-together or not)
        List<String> exceptions = Lists.newArrayList();
        List<EconomyDTOs.CommoditySoldTO> list = topologyDTO.getCommoditySoldListList().stream()
            .filter(commSold -> commSold.getActive())
            .filter(commSold -> !isBicliqueCommodity(commSold.getCommodityType()))
            .filter(commSold -> includeGuaranteedBuyer
                || !AnalysisUtil.GUARANTEED_SELLER_TYPES.contains(topologyDTO.getEntityType())
                || !AnalysisUtil.VDC_COMMODITY_TYPES.contains(commSold.getCommodityType().getType()))
            .map(commSold -> {
                try {
                    return commoditySold(commSold);
                } catch (InvalidTopologyException e) {
                    exceptions.add(e.getMessage());
                    return null;
                }
            })
            .collect(Collectors.toList());
        if (!exceptions.isEmpty()) {
            throw new InvalidTopologyException(
                topologyDTO.getDisplayName() + "[oid="
                        + topologyDTO.getOid() + "] : " + exceptions);
        }

        // In the case of non-shop-together, create the biclique commodities
        if (!shopTogether) {
            list.addAll(bcCommoditiesSold(topologyDTO));
        }
        return list;
    }

    @Nonnull
    private Set<EconomyDTOs.CommoditySoldTO> bcCommoditiesSold(
                    @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO) {
        Set<String> bcKeys = bicliquer.getBcKeys(String.valueOf(topologyDTO.getOid()));
        return bcKeys != null
                    ? bcKeys.stream()
                        .map(this::newBiCliqueCommoditySoldDTO)
                        .collect(Collectors.toSet())
                    : EMPTY_SET;
    }

    @Nonnull
    private EconomyDTOs.CommoditySoldTO commoditySold(
                    @Nonnull final TopologyDTO.CommoditySoldDTO topologyCommSold)
                                    throws InvalidTopologyException {
        final CommodityType commodityType = topologyCommSold.getCommodityType();
        final float capacity = (float)topologyCommSold.getCapacity();
        float used = (float)topologyCommSold.getUsed();
        final int type = commodityType.getType();
        if (used > capacity) {
            if (AnalysisUtil.COMMODITIES_TO_CAP.contains(type)) {
                used = capacity * 0.999999f;
            } else if (!(AnalysisUtil.COMMODITIES_TO_SKIP.contains(type) ||
                            AnalysisUtil.ACCESS_COMMODITY_TYPES.contains(type))) {
                throw new InvalidTopologyException(errorMsg(topologyCommSold));
            }
        }
        final EconomyDTOs.CommoditySoldSettingsTO economyCommSoldSettings =
                        EconomyDTOs.CommoditySoldSettingsTO.newBuilder()
                        .setResizable(topologyCommSold.getIsResizeable())
                        .setCapacityIncrement(capacity / 5) // TODO: use the probe DTO's UsedIncrement
                        .setCapacityUpperBound(capacity)
                        .setUtilizationUpperBound(
                            (float)(topologyCommSold.getEffectiveCapacityPercentage() / 100.0))
                        .setPriceFunction(priceFunction(topologyCommSold))
                        .setUpdateFunction(updateFunction(topologyCommSold))
                        .build();
        return EconomyDTOs.CommoditySoldTO.newBuilder()
                        .setPeakQuantity((float)topologyCommSold.getPeak())
                        .setCapacity(capacity)
                        .setQuantity(used)
                        .setSettings(economyCommSoldSettings)
                        .setSpecification(commoditySpecification(commodityType))
                        .setThin(topologyCommSold.getIsThin())
                        .build();
    }

    private static String errorMsg(TopologyDTO.CommoditySoldDTO topologyCommSold) {
        return "used > capacity (commodity type="
                        + topologyCommSold.getCommodityType().getType()
                        + ", used=" + topologyCommSold.getUsed()
                        + ", capacity=" + topologyCommSold.getCapacity()
                        + ")";
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
    private EconomyDTOs.CommoditySoldTO newBiCliqueCommoditySoldDTO(String bcKey) {
        return EconomyDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(bcSpec(bcKey))
                        .setSettings(AnalysisUtil.BC_SETTING_TO)
                        .build();
    }

    @Nonnull
    private EconomyDTOs.CommoditySpecificationTO bcSpec(@Nonnull String bcKey) {
        return EconomyDTOs.CommoditySpecificationTO.newBuilder()
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
        @Nonnull final EconomyDTOs.CommoditySoldTO commSoldTO) {
        return economyToTopologyCommodity(commSoldTO.getSpecification())
                .map(commType -> TopologyDTO.CommoditySoldDTO.newBuilder()
                    .setCapacity(commSoldTO.getCapacity())
                    .setPeak(commSoldTO.getPeakQuantity())
                    .setUsed(commSoldTO.getQuantity())
                    .setIsResizeable(commSoldTO.getSettings().getResizable())
                    .setEffectiveCapacityPercentage(
                        commSoldTO.getSettings().getUtilizationUpperBound() * 100)
                    .setCommodityType(commType)
                    .setIsThin(commSoldTO.getThin())
                    .build());
    }

    @Nonnull
    private EconomyDTOs.CommoditySpecificationTO commoditySpecification(
            @Nonnull final CommodityType topologyCommodity) {
        final EconomyDTOs.CommoditySpecificationTO economyCommodity =
                        EconomyDTOs.CommoditySpecificationTO.newBuilder()
            .setType(commodityType(topologyCommodity))
            .setBaseType(topologyCommodity.getType())
            .setDebugInfoNeverUseInCode(commodityDebugInfo(topologyCommodity))
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
    private int commodityType(@Nonnull final CommodityType commType) {
        int type = commType.getType();
        String key = commType.getKey();
        String allocationKey = type + (key != null ? "|" + key : "");
        return commodityTypeAllocator.allocate(allocationKey);
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
     * @return The {@link Action} describing the recommendation in a topology-specific way.
     */
    @Nonnull
    public Optional<Action> interpretAction(@Nonnull final ActionTO actionTO) {
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
                    infoBuilder.setMove(interpretMoveAction(actionTO.getMove()));
                    break;
                case RECONFIGURE:
                    infoBuilder.setReconfigure(
                            interpretReconfigureAction(actionTO.getReconfigure()));
                    break;
                case PROVISION_BY_SUPPLY:
                    infoBuilder.setProvision(
                            interpretProvisionBySupply(actionTO.getProvisionBySupply()));
                    break;
                case PROVISION_BY_DEMAND:
                    infoBuilder.setProvision(
                                    interpretProvisionByDemand(actionTO.getProvisionByDemand()));
                    break;
                case RESIZE:
                    infoBuilder.setResize(interpretResize(actionTO.getResize()));
                    break;
                case ACTIVATE:
                    infoBuilder.setActivate(interpretActivate(actionTO.getActivate()));
                    break;
                case DEACTIVATE:
                    infoBuilder.setDeactivate(interpretDeactivate(actionTO.getDeactivate()));
                    break;
                default:
                    return Optional.empty();
            }

            action.setInfo(infoBuilder);

            return Optional.of(action.build());
        } catch (RuntimeException e) {
            logger.error(e);
            return Optional.empty();
        }
    }

    private Provision interpretProvisionByDemand(ProvisionByDemandTO provisionByDemandTO) {
        return ActionDTO.Provision.newBuilder()
                        .setEntityToCloneId(provisionByDemandTO.getModelBuyer())
                        .setProvisionedSeller(
                            provisionByDemandTO.getProvisionedSeller()).build();
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
                   ActionDTO.Explanation.DeactivateExplanation.newBuilder().build());
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
                        .addAllReconfigureCommodity(reconfTO.getCommodityToReconfigureList())
                        .build();
    }

    private MoveExplanation interpretMoveExplanation(MoveTO moveTO) {
        MoveExplanation.Builder moveExpBuilder = MoveExplanation.newBuilder();
        ActionDTOs.MoveExplanation marketMoveExp = moveTO.getMoveExplanation();
        switch (marketMoveExp.getExplanationTypeCase()) {
            case COMPLIANCE:
                moveExpBuilder.setCompliance(ActionDTO.Explanation.MoveExplanation.Compliance
                    .newBuilder().addAllMissingCommodities(marketMoveExp
                        .getCompliance().getMissingCommoditiesList())
                    .build());
                break;
            case CONGESTION:
                moveExpBuilder.setCongestion(ActionDTO.Explanation.MoveExplanation.Congestion
                    .newBuilder().addAllCongestedCommodities(marketMoveExp
                        .getCongestion().getCongestedCommoditiesList())
                    .build());
                break;
            case EVACUATION:
                moveExpBuilder.setEvacuation(
                    ActionDTO.Explanation.MoveExplanation.Evacuation.newBuilder()
                        .setSuspendedEntity(
                            marketMoveExp.getEvacuation().getSuspendedTrader())
                        .build());
                break;
            case INITIALPLACEMENT:
                moveExpBuilder.setInitialPlacement(
                    ActionDTO.Explanation.MoveExplanation.InitialPlacement.newBuilder()
                        .build());
                break;
            case PERFORMANCE:
                moveExpBuilder.setPerformance(
                    ActionDTO.Explanation.MoveExplanation.Performance.newBuilder()
                        .build());
                break;
            default:
                logger.error("Unknown explanation for move action");
                break;
        }
        return moveExpBuilder.build();
    }

    @Nonnull
    private ActionDTO.Move interpretMoveAction(@Nonnull final MoveTO moveTO) {
        final ShoppingListInfo shoppingList =
                        shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        if (shoppingList == null) {
            throw new IllegalStateException(
                            "Market returned invalid shopping list for MOVE: " + moveTO);
        } else {
            return ActionDTO.Move.newBuilder().setTargetId(shoppingList.buyerId)
                            .setSourceId(moveTO.getSource())
                            .setDestinationId(moveTO.getDestination()).build();
        }
    }

    @Nonnull
    private ActionDTO.Reconfigure interpretReconfigureAction(
                    @Nonnull final ReconfigureTO reconfigureTO) {
        final ShoppingListInfo shoppingList =
                        shoppingListOidToInfos.get(reconfigureTO.getShoppingListToReconfigure());
        if (shoppingList == null) {
            throw new IllegalStateException(
                "Market returned invalid shopping list for RECONFIGURE: " + reconfigureTO);
        } else {
            return ActionDTO.Reconfigure.newBuilder()
                .setTargetId(shoppingList.buyerId)
                .setSourceId(reconfigureTO.getSource()).build();
        }
    }

    @Nonnull
    private ActionDTO.Provision interpretProvisionBySupply(
                    @Nonnull final ProvisionBySupplyTO provisionBySupplyTO) {
        return ActionDTO.Provision.newBuilder()
            .setEntityToCloneId(provisionBySupplyTO.getModelSeller())
            .setProvisionedSeller(provisionBySupplyTO.getProvisionedSeller()).build();
    }

    @Nonnull
    private ActionDTO.Resize interpretResize(@Nonnull final ResizeTO resizeTO) {
        final long entityId = resizeTO.getSellingTrader();
        final CommodityType topologyCommodity =
                economyToTopologyCommodity(resizeTO.getSpecification())
                    .orElseThrow(() -> new IllegalArgumentException(
                        "Resize commodity can't be converted to topology commodity format! "
                            + resizeTO.getSpecification()));
        return ActionDTO.Resize.newBuilder()
                .setTargetId(entityId)
                .setNewCapacity(resizeTO.getNewCapacity())
                .setOldCapacity(resizeTO.getOldCapacity())
                .setCommodityType(topologyCommodity)
                .build();
    }

    @Nonnull
    private ActionDTO.Activate interpretActivate(@Nonnull final ActivateTO activateTO) {
        final long entityId = activateTO.getTraderToActivate();
        final List<CommodityType> topologyCommodities =
                activateTO.getTriggeringBasketList().stream()
                        .map(this::economyToTopologyCommodity)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());
        return ActionDTO.Activate.newBuilder()
                .setTargetId(entityId)
                .addAllTriggeringCommodities(topologyCommodities)
                .build();
    }

    @Nonnull
    private ActionDTO.Deactivate interpretDeactivate(@Nonnull final DeactivateTO deactivateTO) {
        final long entityId = deactivateTO.getTraderToDeactivate();
        final List<CommodityType> topologyCommodities =
                deactivateTO.getTriggeringBasketList().stream()
                        .map(this::economyToTopologyCommodity)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());
        return ActionDTO.Deactivate.newBuilder()
                .setTargetId(entityId)
                .addAllTriggeringCommodities(topologyCommodities)
                .build();
    }

    @VisibleForTesting
    @Nonnull
    Optional<CommodityType> economyToTopologyCommodity(
            @Nonnull final EconomyDTOs.CommoditySpecificationTO economyCommodity) {
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
     * The {@link EconomyDTOs.CommoditySpecificationTO}s we get back from the market aren't exactly
     * equal to the ones we pass in - for example, the debug info may be absent. We only want to
     * compare the type and base_type.
     */
    private static class EconomyCommodityId {
        final int type;
        final int baseType;

        EconomyCommodityId(@Nonnull final EconomyDTOs.CommoditySpecificationTO economyCommodity) {
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
                    new ShoppingListInfo(l.getNewShoppingList(), l.getBuyer(), -1,
                        TopologyDTO.TopologyEntityDTO.CommodityBoughtList.newBuilder()
                            .build())));
        return;
    }
}
