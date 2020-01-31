package com.vmturbo.market.topology.conversions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.market.topology.conversions.ConversionErrorCounts.ErrorCategory;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO.Builder;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Commodity converter class is used to convert CommoditySoldDTOs sold by the TopologyEntityDTOs
 * to CommoditySoldTOs. This class uses the same commodityTypeAllocator, commoditySpecMap,
 * dsBasedBicliquer and numConsumersOfSoldCommTable as TopologyConverter.
 */
public class CommodityConverter {
    private static final Logger logger = LogManager.getLogger();
    private final NumericIDAllocator commodityTypeAllocator;
    // Mapping of CommoditySpecificationTO (string representation of type and baseType from
    // CommoditySpecificationTO) to specific CommodityType.
    private final Map<String, CommodityType> commoditySpecMap;
    private final boolean includeGuaranteedBuyer;
    private final BiCliquer dsBasedBicliquer;
    private int bcBaseType = -1;
    private final Table<Long, CommodityType, Integer> numConsumersOfSoldCommTable;
    private final ConversionErrorCounts conversionErrorCounts;
    private final ConsistentScalingHelper consistentScalingHelper;

    CommodityConverter(@Nonnull final NumericIDAllocator commodityTypeAllocator,
                       @Nonnull final Map<String, CommodityType> commoditySpecMap,
                       final boolean includeGuaranteedBuyer,
                       @Nonnull final BiCliquer dsBasedBicliquer,
                       @Nonnull final Table<Long, CommodityType, Integer> numConsumersOfSoldCommTable,
                       @Nonnull final ConversionErrorCounts conversionErrorCounts,
                       @Nonnull final ConsistentScalingHelper consistentScalingHelper) {
        this.commodityTypeAllocator = commodityTypeAllocator;
        this.commoditySpecMap = commoditySpecMap;
        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
        this.dsBasedBicliquer = dsBasedBicliquer;
        this.numConsumersOfSoldCommTable = numConsumersOfSoldCommTable;
        this.conversionErrorCounts = conversionErrorCounts;
        this.consistentScalingHelper = consistentScalingHelper;
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
                        || !MarketAnalysisUtils.GUARANTEED_SELLER_TYPES.contains(topologyDTO.getEntityType())
                        || !MarketAnalysisUtils.VDC_COMMODITY_TYPES.contains(commSold.getCommodityType().getType()))
                .map(commoditySoldDTO -> createCommonCommoditySoldTO(commoditySoldDTO, topologyDTO))
                .collect(Collectors.toList());
        return list;
    }

    /**
     * Creates a {@link CommoditySoldTO} from the {@link TopologyDTO.CommoditySoldDTO}.
     *
     * @param topologyCommSold the source {@link CommoditySoldDTO}
     * @param dto the {@link TopologyEntityDTO} selling the commSold
     * @return a {@link CommoditySoldTO}
     */
    @Nonnull
    CommodityDTOs.CommoditySoldTO createCommonCommoditySoldTO(
        @Nonnull final CommoditySoldDTO topologyCommSold,
        @Nonnull TopologyEntityDTO dto) {
        final CommodityType commodityType = topologyCommSold.getCommodityType();
        float capacity = (float)topologyCommSold.getCapacity();
        float used = getUsedValue(topologyCommSold, TopologyDTO.CommoditySoldDTO::getUsed,
                                  topologyCommSold.hasHistoricalUsed()
                                                  ? TopologyDTO.CommoditySoldDTO::getHistoricalUsed
                                                  : null);
        // if this commodity has a scaling factor set, then scale up the
        // USED and CAPACITY by scalingFactor for use in the new CommoditySoldTO
        final float scalingFactor = (float)topologyCommSold.getScalingFactor();
        if (topologyCommSold.hasScalingFactor() && logger.isDebugEnabled()) {
            logger.debug("Scaling comm {}, factor {}, for topology entity {},"
                         + " prev used {}, new {}, prev capacity {}, new {}",
                    commodityType.getType(), scalingFactor, dto.getDisplayName(),
                    used, used * scalingFactor, capacity, capacity * scalingFactor
            );
        }
        capacity *= scalingFactor;
        used *= scalingFactor;
        final int type = commodityType.getType();
        final CommodityDTO.CommodityType sdkCommType = CommodityDTO.CommodityType.forNumber(type);
        final String comName = sdkCommType == null ? "UNKNOWN" : sdkCommType.name();
        boolean resizable = topologyCommSold.getIsResizeable();
        boolean capacityNaN = Double.isNaN(topologyCommSold.getCapacity());
        boolean usedNaN = Double.isNaN(topologyCommSold.getUsed());
        if (capacityNaN && usedNaN) {
            resizable = true;
            logger.warn("Setting resizable true for {} of entity {}",
                        comName, dto.getDisplayName());
        }
        if (used < 0) {
            if (logger.isDebugEnabled() || used != -1) {
                // We don't want to log every time we get used = -1 because mediation
                // sets some values to -1 as default.
                logger.warn("Setting negative used value to 0 for {} of entity {}.",
                            comName, dto.getDisplayName());
            }
            used = 0f;
        } else if (used > capacity) {
            if (MarketAnalysisUtils.COMMODITIES_TO_CAP.contains(type)) {
                float cappedUsed = capacity * TopologyConversionConstants.CAPACITY_FACTOR;
                conversionErrorCounts.recordError(ErrorCategory.USED_GT_CAPACITY_MEDIATION,
                                                  comName);
                logger.trace("Used > Capacity for {} of entity {}. "
                             + " Used: {}, Capacity: {}, Capped used: {}."
                             + " This is a mediation error and should be looked at.",
                             comName, dto.getDisplayName(), used, capacity, cappedUsed);
                used = cappedUsed;
            } else if (MarketAnalysisUtils.VALID_COMMODITIES_TO_CAP.contains(type)) {
                float cappedUsed = capacity * TopologyConversionConstants.CAPACITY_FACTOR;
                logger.trace("Used > Capacity for {} of entity {}. "
                             + " Used: {}, Capacity: {}, Capped used: {}."
                             + " Capping the used to be less than capacity.",
                             comName, dto.getDisplayName(), used, capacity, cappedUsed);
                used = cappedUsed;
            } else if (!(MarketAnalysisUtils.COMMODITIES_TO_SKIP.contains(type) ||
                    MarketAnalysisUtils.ACCESS_COMMODITY_TYPES.contains(type))) {
                conversionErrorCounts.recordError(ErrorCategory.USED_GT_CAPACITY, comName);
                logger.trace("Used > Capacity for {} of entity {}. Used: {}, Capacity: {}",
                             comName, dto.getDisplayName(), used, capacity);
            }
        }

        // effective capacity percentage are overloaded with 2 functionality.
        // when the value is less than 100 it is used as utilizationUpperBound which
        // will reduce the effective capacity.
        // when the value is greater than 100 it is used to scale the utilization.
        // Even though increasing the capacity and decreasing the utilization are
        // effectively the same we wanted to do it this way to prevent vm from
        // resizing above the host capacity.
        float effectiveCapacityPercentage =
                (float)(topologyCommSold.getEffectiveCapacityPercentage() / 100.0);
        float utilizationUpperBound = effectiveCapacityPercentage > 1.0f ?
                1.0f : effectiveCapacityPercentage;
        float scale = effectiveCapacityPercentage < 1.0f ?
                1.0f : effectiveCapacityPercentage;
        final CommodityDTOs.CommoditySoldSettingsTO economyCommSoldSettings =
                CommodityDTOs.CommoditySoldSettingsTO.newBuilder()
                        .setResizable(resizable && !MarketAnalysisUtils.PROVISIONED_COMMODITIES.contains(type)
                                && !TopologyConversionUtils.isEntityConsumingCloud(dto)
                                // We do not want to resize idle entities. If the resizable flag
                                // is not set to false for idle entities, they can get resized
                                // because of hitorical utilization.
                                && dto.getEntityState() == EntityState.POWERED_ON)
                        .setCapacityIncrement(topologyCommSold.getCapacityIncrement())
                        .setCapacityUpperBound(capacity)
                        .setUtilizationUpperBound(utilizationUpperBound)
                        .setPriceFunction(priceFunction(topologyCommSold.getCommodityType(),
                                scale))
                        .setUpdateFunction(updateFunction(topologyCommSold))
                        .build();

        // not mandatory (e.g. for access commodities)
        double maxQuantity = topologyCommSold.hasHistoricalUsed()
                             && topologyCommSold.getHistoricalUsed().hasMaxQuantity()
                                             ? topologyCommSold.getHistoricalUsed().getMaxQuantity()
                                             : 0;
        float maxQuantityFloat;
        if (maxQuantity < 0) {
            conversionErrorCounts.recordError(ErrorCategory.MAX_QUANTITY_NEGATIVE, comName);
            logger.trace("maxQuantity: {} is less than 0. Setting it 0 for {} of entity {}",
                         maxQuantity, comName, dto.getDisplayName());
            maxQuantityFloat = 0;
        } else {
            maxQuantityFloat = (float)maxQuantity;
            if (maxQuantityFloat < 0) {
                logger.warn("Float to double cast error. maxQuantity:{}. maxQuantityFloat:{}."
                            + " for {} of entity {}",
                            maxQuantity, maxQuantityFloat, comName, dto.getDisplayName());
                maxQuantityFloat = 0;
            }
        }
        // if entry not present, initialize to 0
        int numConsumers = Optional.ofNullable(numConsumersOfSoldCommTable.get(dto.getOid(),
                topologyCommSold.getCommodityType())).map(o -> o.intValue()).orElse(0);
        float peak = getUsedValue(topologyCommSold, TopologyDTO.CommoditySoldDTO::getPeak,
                                  topologyCommSold.hasHistoricalPeak()
                                                  ? TopologyDTO.CommoditySoldDTO::getHistoricalPeak
                                                  : null);
        final Builder soldCommBuilder = CommoditySoldTO.newBuilder();
        soldCommBuilder.setPeakQuantity(peak)
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
        // Set the historical quantity for the onPrem
        // right sizing only if the percentile value is set.
        if (topologyCommSold.hasHistoricalUsed()
                && topologyCommSold.getHistoricalUsed().hasPercentile()) {
            logger.debug("Using percentile {} for {} in {}",
                    topologyCommSold.getHistoricalUsed().getPercentile(),
                    topologyCommSold.getCommodityType().getType(),
                    dto.getDisplayName());
            soldCommBuilder.setHistoricalQuantity((float)(topologyCommSold.getCapacity()
                            * topologyCommSold.getScalingFactor()
                            * topologyCommSold.getHistoricalUsed().getPercentile()));
        }
        return soldCommBuilder.build();
    }



    /**
     * Creates a {@link CommoditySoldTO} of specific type with a specific used and capacity.
     *
     * @param commodityType {@link CommodityType} of commodity to be created
     * @param capacity is the capacity of the commSold
     * @param used is the current used of the commSold
     * @param uf is the updating function of the commold
     * @return a {@link CommoditySoldTO}
     */
    @Nonnull
    public CommodityDTOs.CommoditySoldTO createCommoditySoldTO(
            @Nonnull CommodityType commodityType,
            float capacity,
            float used, @Nonnull UpdatingFunctionTO uf) {
        final CommodityDTOs.CommoditySoldSettingsTO economyCommSoldSettings =
                CommodityDTOs.CommoditySoldSettingsTO.newBuilder()
                        .setResizable(false)
                        .setCapacityUpperBound(capacity)
                        .setPriceFunction(priceFunction(commodityType, 1.0f))
                        .setUpdateFunction(uf)
                        .build();

        return CommodityDTOs.CommoditySoldTO.newBuilder()
                .setPeakQuantity(0)
                .setCapacity(capacity)
                .setQuantity(used)
                // Warning: we are down casting from double to float.
                // Market has to change this field to double
                .setSettings(economyCommSoldSettings)
                .setSpecification(commoditySpecification(commodityType))
                .setThin(false)
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
                        .setCloneWithNewType(MarketAnalysisUtils.CLONE_COMMODITIES_WITH_NEW_TYPE
                                .contains(topologyCommodity.getType()))
                        .build();
        commoditySpecMap.put(getKeyFromCommoditySpecification(economyCommodity), topologyCommodity);
        return economyCommodity;
    }

    /**
     * Return specific key based on type and base type of CommoditySpecificationTO.
     * @param economyCommodity to generate key from.
     * @return generated key.
     */
    private String getKeyFromCommoditySpecification(
            @Nonnull final CommodityDTOs.CommoditySpecificationTO economyCommodity) {
        return String.valueOf(economyCommodity.getType()) +
                    TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR +
                    String.valueOf(economyCommodity.getBaseType());
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
                .setType(commodityTypeAllocator.allocate(
                    CommodityDTO.CommodityType.BICLIQUE_VALUE
                    + TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR + bcKey))
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
     * Concatenates the type and the key of the {@link CommodityType}.
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
     * @param commType a commodity type for which to add a price function
     * @param scale    float that represents how much the utilization is scaled to.
     * @return a (reusable) instance of PriceFunctionTO to use in the commodity sold settings.
     */
    @Nonnull
    private static PriceFunctionDTOs.PriceFunctionTO priceFunction(CommodityType commType,
                                                                   float scale) {
        // logic to choose correct price function is based on commodity type
        return MarketAnalysisUtils.priceFunction(commType, scale);
    }

    /**
     * Select the right {@link UpdatingFunctionTO} based on the commodity sold type.
     *
     * @param topologyCommSold a commodity sold for which to add an updating function
     * @return a (reusable) instance of UpdatingFunctionTO to use in the commodity sold settings.
     */
    @Nonnull
    private static UpdatingFunctionTO
                    updateFunction(TopologyDTO.CommoditySoldDTO topologyCommSold) {
        return updateFunction(topologyCommSold.getCommodityType());
    }

    /**
     * Select the right {@link UpdatingFunctionTO} based on the commodity sold type.
     *
     * @param commodityType {@link CommodityType} for which to add an updating function
     * @return a (reusable) instance of UpdatingFunctionTO to use in the commodity sold settings.
     */
    @Nonnull
    private static UpdatingFunctionTO updateFunction(CommodityType commodityType) {
        return MarketAnalysisUtils.updateFunction(commodityType);
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
                .setSettings(MarketAnalysisUtils.BC_SETTING_TO)
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
                commoditySpecMap.get(getKeyFromCommoditySpecification(economyCommodity));
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
                    .setKey(commodityTypeString.substring(separatorIndex + 1))
                    .build();
        } else {
            return CommodityType.newBuilder()
                    .setType(Integer.parseInt(commodityTypeString))
                    .build();
        }
    }

    /**
     * Returns the historical value for sold and bought commodity with the percentile value given
     * a higher priority.
     * @param commDto the sold commodity or the bought commodity.
     * @param usedExtractor function that extracts the current used value.
     * @param historicalExtractor function that extracts the historical utilization(used) value.
     * @return the historical used value, returns null if historical value is not set
     *          and the used extractor is not passed.
     */
    @Nullable
    public static Float getHistoricalUsedOrPeak(final CommodityBoughtDTO commDto,
                                                @Nullable Function<CommodityBoughtDTO, Double> usedExtractor,
                                                @Nullable Function<CommodityBoughtDTO, HistoricalValues> historicalExtractor ) {
        if (historicalExtractor != null) {
            HistoricalValues hv = historicalExtractor.apply(commDto);
            if (hv.hasPercentile()) {
                float value = (float)hv.getPercentile();
                logger.debug("Using percentile value {} for recalculating resize capacity for {}",
                        value, commDto.getCommodityType().getType());
                return value;
            } else if (hv.hasHistUtilization()) {
                // if not then hist utilization which is the historical used value.
                float value = (float)hv.getHistUtilization();
                logger.debug("Using hist Utilization value {} for recalculating resize capacity for {}",
                        value, commDto.getCommodityType().getType());
                return value;
            }
        }
        // otherwise take real-time 'used'
        // real-time values have 0 defaults so there cannot be nulls
        if (usedExtractor != null) {
            float value = usedExtractor.apply(commDto).floatValue();
            logger.debug("Using current used value {} for recalculating resize capacity for {}",
                    value, commDto.getCommodityType().getType());
            return value;
        } else {
            return null;
        }
    }


    /**
     * Fetch the single used or peak value for a topology commodity.
     * System load > history utilization > real-time usage.
     *
     * @param <DtoType> sold or bought topology commodity dto type
     * @param commDto sold or bought commodity dto
     * @param usedExtractor how to get real-time usage from dto
     * @param historicalExtractor how to get history usage from dto, null if dto has no history values
     * @return non-null value (ultimately there are 0 defaults)
     */
    public static <DtoType> float
           getUsedValue(@Nonnull DtoType commDto,
                        @Nonnull Function<DtoType, Double> usedExtractor,
                        @Nullable Function<DtoType, HistoricalValues> historicalExtractor) {
        if (historicalExtractor != null) {
            HistoricalValues hv = historicalExtractor.apply(commDto);
            if (hv.hasHistUtilization()) {
                // if present then hist utilization
                return (float)hv.getHistUtilization();
            }
        }
        // otherwise take real-time 'used'
        // real-time values have 0 defaults so there cannot be nulls
        return usedExtractor.apply(commDto).floatValue();
    }

}
