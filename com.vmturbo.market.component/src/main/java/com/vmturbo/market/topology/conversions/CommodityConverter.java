package com.vmturbo.market.topology.conversions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Commodity converter class is used to convert CommoditySoldDTOs sold by the TopologyEntityDTOs
 * to CommoditySoldTOs. This class uses the same commodityTypeAllocator, commoditySpecMap,
 * dsBasedBicliquer and numConsumersOfSoldCommTable as TopologyConverter.
 */
public class CommodityConverter {
    private static final Logger logger = LogManager.getLogger();
    private final NumericIDAllocator commodityTypeAllocator;
    private final Map<EconomyCommodityId, CommodityType> commoditySpecMap;
    private final boolean includeGuaranteedBuyer;
    private final BiCliquer dsBasedBicliquer;
    private int bcBaseType = -1;
    private final Table<Long, CommodityType, Integer> numConsumersOfSoldCommTable;

    CommodityConverter(NumericIDAllocator commodityTypeAllocator,
                       Map<EconomyCommodityId, CommodityType> commoditySpecMap,
                       boolean includeGuaranteedBuyer, BiCliquer dsBasedBicliquer,
                       Table<Long, CommodityType, Integer> numConsumersOfSoldCommTable) {
        this.commodityTypeAllocator = commodityTypeAllocator;
        this.commoditySpecMap = commoditySpecMap;
        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
        this.dsBasedBicliquer = dsBasedBicliquer;
        this.numConsumersOfSoldCommTable = numConsumersOfSoldCommTable;
    }

    /**
     * Creates a collection of {@link CommoditySoldTO} from the commodities sold by the
     * TopologyEntityDTO.
     *
     * @param topologyDTO the source {@link TopologyDTO.TopologyEntityDTO}
     * @return a collection of {@link CommoditySoldTO}
     */
    @Nonnull
    public Collection<CommoditySoldTO> commoditiesSoldList(
            @Nonnull final TopologyDTO.TopologyEntityDTO topologyDTO) {
        // DSPMAccess and Datastore commodities are always dropped (shop-together or not)
        List<CommoditySoldTO> list = topologyDTO.getCommoditySoldListList().stream()
                .filter(commSold -> commSold.getActive())
                .filter(commSold -> !isBicliqueCommodity(commSold.getCommodityType()))
                .filter(commSold -> includeGuaranteedBuyer
                        || !AnalysisUtil.GUARANTEED_SELLER_TYPES.contains(topologyDTO.getEntityType())
                        || !AnalysisUtil.VDC_COMMODITY_TYPES.contains(commSold.getCommodityType().getType()))
                .map(commoditySoldDTO -> createCommonCommoditySoldTO(commoditySoldDTO, topologyDTO))
                .collect(Collectors.toList());
        return list;
    }

    /**
     * Creates a {@link CommoditySoldTO} from the {@link TopologyDTO.CommoditySoldDTO}
     *
     * @param topologyCommSold the source {@link TopologyDTO.CommoditySoldDTO}
     * @param dto the {@link TopologyDTO.TopologyEntityDTO} selling the commSold
     * @return a {@link CommoditySoldTO}
     */
    @Nonnull
    private CommodityDTOs.CommoditySoldTO createCommonCommoditySoldTO(
            @Nonnull final TopologyDTO.CommoditySoldDTO topologyCommSold,
            @Nonnull TopologyDTO.TopologyEntityDTO dto) {
        final CommodityType commodityType = topologyCommSold.getCommodityType();
        float capacity = (float)topologyCommSold.getCapacity();
        float used = (float)topologyCommSold.getUsed();
        // if this commodity has a scaling factor set, then scale up the
        // USED and CAPACITY by scalingFactor for use in the new CommoditySoldTO
        if (topologyCommSold.hasScalingFactor()) {
            final float scalingFactor = (float) topologyCommSold.getScalingFactor();
            if (logger.isDebugEnabled()) {
                logger.debug("Scaling up comm {}, factor {}, for topology entity {}, prev used {}, new {}," +
                                "prev capacity {}, new {}",
                        commodityType.getType(), scalingFactor, dto.getDisplayName(),
                        used, used * scalingFactor, capacity, capacity * scalingFactor
                );
            }
            capacity *= scalingFactor;
            used *= scalingFactor;
        }
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
                float cappedUsed = capacity * TopologyConversionConstants.CAPACITY_FACTOR;
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

    /**
     * Create biclique commodity sold for entities. The commodity sold will play a role
     * in shop alone placement.
     *
     * @param oid the oid of the entity who should sell a biclique commodities
     * @return a set of biclique commodity sold DTOs
     */
    @Nonnull
    public Set<CommoditySoldTO> bcCommoditiesSold(long oid) {
        Set<String> bcKeys = dsBasedBicliquer.getBcKeys(String.valueOf(oid));
        return bcKeys != null
                ? bcKeys.stream()
                .map(this::newBiCliqueCommoditySoldDTO)
                .collect(Collectors.toSet())
                : Collections.emptySet();
    }

    /**
     * Creates a {@link CommoditySpecificationTO} from a {@link CommodityType} and populates
     * the commoditySpecMap with commSpecTO to CommodityType mapping.
     *
     * @param topologyCommodity the CommodityType for which the CommSpecTO is to be created
     * @return the {@link CommoditySpecificationTO} for the {@link CommodityType}
     */
    @Nonnull
    public CommodityDTOs.CommoditySpecificationTO commoditySpecification(
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
     * Create a CommSpecTO for a biclique key.
     *
     * @param bcKey the biClique key for which CommSpecTO is to be created
     * @return the {@link CommoditySpecificationTO} for the biClique key
     */
    @Nonnull
    public CommodityDTOs.CommoditySpecificationTO bcSpec(@Nonnull String bcKey) {
        return CommodityDTOs.CommoditySpecificationTO.newBuilder()
                .setBaseType(bcBaseType())
                .setType(commodityTypeAllocator.allocate(bcKey))
                .setDebugInfoNeverUseInCode(TopologyConversionConstants.BICLIQUE + " " + bcKey)
                .build();
    }

    /**
     * Is {@link CommodityType} a biClique Commodity?
     *
     * @param commodityType the commodityType to be checked
     * @return True if the type is DSPM or DATASTORE commodity and it has a key. False otherwise.
     */
    public static boolean isBicliqueCommodity(CommodityType commodityType) {
        boolean isOfType = AnalysisUtil.DSPM_OR_DATASTORE.contains(commodityType.getType());
        return isOfType && commodityType.hasKey();
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

    /**
     * Concatenates the type and the key of the {@link CommodityType}
     *
     * @param commType the {@link CommodityType} for which string conversion is desired
     * @return string conversion of {@link CommodityType}
     */
    @Nonnull
    private String commodityTypeToString(@Nonnull final CommodityType commType) {
        int type = commType.getType();
        return type + (commType.hasKey() ?
                TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR + commType.getKey()
                : "");
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
     * Creates a {@link CommoditySoldTO} for the biClique key passed in with default
     * biClique settings.
     *
     * @param bcKey the biClique key for which {@link CommoditySoldTO} is to be created
     * @return the {@link CommoditySoldTO} for the biClique key.
     */
    @Nonnull
    private CommodityDTOs.CommoditySoldTO newBiCliqueCommoditySoldDTO(String bcKey) {
        return CommodityDTOs.CommoditySoldTO.newBuilder()
                .setSpecification(bcSpec(bcKey))
                .setSettings(AnalysisUtil.BC_SETTING_TO)
                .build();
    }

    /**
     * The base type of bicliques. Allocates a new type if not already allocated.
     * @return An integer representing the base type of biCliques.
     */
    private int bcBaseType() {
        if (bcBaseType == -1) {
            bcBaseType = commodityTypeAllocator.allocate(TopologyConversionConstants.BICLIQUE);
        }
        return bcBaseType;
    }

    /**
     * Gets the name of the commodity from the id.
     *
     * @param commodityId the commodity id for which the name is needed
     * @return the name of the commodity
     */
    String getCommodityName(int commodityId) {
        return commodityTypeAllocator.getName(commodityId);
    }

    @VisibleForTesting
    @Nonnull
    Optional<CommodityType> economyToTopologyCommodity(
            @Nonnull final CommodityDTOs.CommoditySpecificationTO economyCommodity) {
        final CommodityType topologyCommodity =
                commoditySpecMap.get(new EconomyCommodityId(economyCommodity));
        if (topologyCommodity == null) {
            if (commodityTypeAllocator.getName(economyCommodity.getBaseType()).equals(
                    TopologyConversionConstants.BICLIQUE)) {
                // this is a biclique commodity
                return Optional.empty();
            }
            throw new IllegalStateException("Market returned invalid commodity specification " +
                    economyCommodity + "! " +
                    "Registered ones are " + commoditySpecMap.keySet());
        }
        return Optional.of(topologyCommodity);
    }

    @VisibleForTesting
    @Nonnull
    CommodityType commodityIdToCommodityType(final int marketCommodityId) {
        return stringToCommodityType(getCommodityName(marketCommodityId));
    }

    @Nonnull
    private CommodityType stringToCommodityType(@Nonnull final String commodityTypeString) {
        int separatorIndex = commodityTypeString.indexOf(TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR);
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
}
