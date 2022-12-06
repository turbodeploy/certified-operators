package com.vmturbo.market.topology.conversions;

import static com.vmturbo.market.topology.TopologyConversionConstants.TIMESLOT_COMMODITIES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.topology.TopologyDTO.*;
import com.vmturbo.platform.common.dto.CommonDTO;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.market.topology.conversions.ConversionErrorCounts.ErrorCategory;
import com.vmturbo.market.topology.conversions.TopologyConverter.UsedAndPeak;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO.Builder;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Commodity converter class is used to convert CommoditySoldDTOs sold by the TopologyEntityDTOs
 * to CommoditySoldTOs. This class uses the same commodityTypeAllocator, commoditySpecMap,
 * dsBasedBicliquer and numConsumersOfSoldCommTable as TopologyConverter.
 */
public class CommodityConverter {
    private static final Logger logger = LogManager.getLogger();
    private final CommodityTypeAllocator commodityTypeAllocator;

    private final boolean includeGuaranteedBuyer;
    private final BiCliquer dsBasedBicliquer;

    private final Table<Long, CommodityType, Integer> numConsumersOfSoldCommTable;
    private final ConversionErrorCounts conversionErrorCounts;
    private int licensePriceWeightScale = MarketAnalysisUtils.PRICE_WEIGHT_SCALE;

    // provider oid -> commodity type -> used value of all consumers to be removed of this provider
    private Map<Long, Map<CommodityType, UsedAndPeak>> providerUsedSubtractionMap = Collections.emptyMap();

    private final boolean enableOP;

    private final TopologyInfo topologyInfo;

    CommodityConverter(@Nonnull final NumericIDAllocator idAllocator,
                       final boolean includeGuaranteedBuyer,
                       @Nonnull final BiCliquer dsBasedBicliquer,
                       @Nonnull final Table<Long, CommodityType, Integer> numConsumersOfSoldCommTable,
                       @Nonnull final ConversionErrorCounts conversionErrorCounts,
                       @Nonnull final ConsistentScalingHelper consistentScalingHelper,
                       final int licensePriceWeightScale,
                       final boolean enableOP,
                       final TopologyInfo topologyInfo) {
        this.commodityTypeAllocator = new CommodityTypeAllocator(idAllocator);

        this.includeGuaranteedBuyer = includeGuaranteedBuyer;
        this.dsBasedBicliquer = dsBasedBicliquer;
        this.numConsumersOfSoldCommTable = numConsumersOfSoldCommTable;
        this.conversionErrorCounts = conversionErrorCounts;
        this.licensePriceWeightScale = licensePriceWeightScale;
        this.enableOP = enableOP;
        this.topologyInfo = topologyInfo;
    }

    /**
     * Returns the {@link CommodityTypeAllocator} associated with a {@link CommodityConverter}.
     *
     * @return CommodityTypeAllocator
     */
    public CommodityTypeAllocator getCommTypeAllocator() {
        return commodityTypeAllocator;
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

        final long numLicenses = (topologyDTO.getEntityType() == CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE)
                ? topologyDTO.getCommoditySoldListList().stream().filter(cs -> cs.getCommodityType().getType()
                        == CommodityDTO.CommodityType.SOFTWARE_LICENSE_COMMODITY_VALUE).count() : 0;

        // DSPMAccess and Datastore commodities are always dropped (shop-together or not)
        final List<CommoditySoldTO> list = topologyDTO.getCommoditySoldListList().stream()
                .filter(commSold -> commSold.getActive())
                .filter(commSold -> !isBicliqueCommodity(commSold.getCommodityType()))
                .filter(commSold -> includeGuaranteedBuyer
                        || !MarketAnalysisUtils.GUARANTEED_SELLER_TYPES.contains(topologyDTO.getEntityType())
                        || !MarketAnalysisUtils.VDC_COMMODITY_TYPES.contains(commSold.getCommodityType().getType()))
                .map(commoditySoldDTO -> createCommonCommoditySoldTOList(commoditySoldDTO, topologyDTO,
                        licensePriceWeightScale * numLicenses))
                .flatMap(List::stream)
                .collect(Collectors.toList());
        return list;
    }

    /**
     * Creates a list of {@link CommoditySoldTO} from the {@link TopologyDTO.CommoditySoldDTO}.
     *
     * @param topologyCommSold the source {@link CommoditySoldDTO}
     * @param dto the {@link TopologyEntityDTO} selling the commSold
     * @additionalSoldWeight is the weight for soldCommodities.
     * @return a list of {@link CommoditySoldTO}
     */
    @Nonnull
    List<CommodityDTOs.CommoditySoldTO> createCommonCommoditySoldTOList(
        @Nonnull final CommoditySoldDTO topologyCommSold,
        @Nonnull TopologyEntityDTO dto,
        long additionalSoldWeight) {
        final CommodityType commodityType = topologyCommSold.getCommodityType();
        float capacity = (float)topologyCommSold.getCapacity();
        if (!topologyCommSold.hasCapacity() || capacity <= 0) {
            if (MarketAnalysisUtils.ACCESS_COMMODITY_TYPES.contains(commodityType.getType())) {
                capacity = (float)SDKConstants.ACCESS_COMMODITY_CAPACITY;
            } else {
                // such an invalid entity should be not controllable
                Level level = dto.getAnalysisSettings().getControllable() ? Level.WARN : Level.DEBUG;
                logger.log(level, "Capacity is unset for topology entity {} commodity {}",
                                dto.getDisplayName(), commodityType.getType());
                capacity = 0;
            }
        }
        float used = getUsedValue(topologyCommSold, TopologyDTO.CommoditySoldDTO::getUsed,
                topologyCommSold.hasHistoricalUsed()
                        ? TopologyDTO.CommoditySoldDTO::getHistoricalUsed
                        : null);
        // if this commodity has a scaling factor set, then scale up the
        // USED and CAPACITY by scalingFactor for use in the new CommoditySoldTO
        final double scalingFactor = topologyCommSold.getScalingFactor();
        if (topologyCommSold.hasScalingFactor() && logger.isDebugEnabled()) {
            logger.debug("Scaling comm {}, factor {}, for topology entity {},"
                            + " prev used {}, new {}, prev capacity {}, new {}",
                    commodityType.getType(), scalingFactor, dto.getDisplayName(),
                    used, used * scalingFactor, capacity, capacity * scalingFactor
            );
        }
        capacity *= scalingFactor;
        used *= scalingFactor;

        boolean shouldSubtractUsageAndPeak = shouldUseSubtractionMap(dto, topologyCommSold);
        // Subtract used value of consumers from used value of provider.
        // This can only happen in a plan with entities to remove.
        if (shouldSubtractUsageAndPeak) {
            float subtractedUsed = providerUsedSubtractionMap.get(dto.getOid()).get(topologyCommSold.getCommodityType()).used;
            logger.debug("provider {} is subtracting {} used for commodity ()", dto.getOid(), subtractedUsed, topologyCommSold.getCommodityType().getType());
            used -= subtractedUsed;
        }

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
        } else if (used > capacity && !TopologyConversionUtils.isVsanStorage(dto)) {
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

        resizable = resizable && !MarketAnalysisUtils.PROVISIONED_COMMODITIES.contains(type)
                && !TopologyConversionUtils.isEntityConsumingCloud(dto)
                // We do not want to resize idle entities. If the resizable flag
                // is not set to false for idle entities, they can get resized
                // because of historical utilization.
                && dto.getEntityState() == EntityState.POWERED_ON;

        // Overwrite the flag for vSAN
        if ((TopologyConversionUtils.isVsanStorage(dto)
                && type == CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE) ||
                (dto.getEntityType() == EntityType.CLUSTER_VALUE
                        && (type == CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE ||
                        type == CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE))) {
            resizable = true;
        }

        resizable = resizable && dto.getAnalysisSettings().getControllable();

        final CommodityDTOs.CommoditySoldSettingsTO.Builder economyCommSoldSettings =
                CommodityDTOs.CommoditySoldSettingsTO.newBuilder()
                        .setResizable(resizable)
                        .setCapacityIncrement((float)(topologyCommSold.getCapacityIncrement() * scalingFactor))
                        .setUtilizationUpperBound(utilizationUpperBound)
                        .setPriceFunction(priceFunction(topologyCommSold.getCommodityType(),
                            scale, dto, additionalSoldWeight, enableOP))
                        .setUpdateFunction(updateFunction(topologyCommSold, commodityTypeAllocator, topologyInfo));

        // Set thresholds for the commodity sold (min/Max of VCPU/VMem for on-prem VMs).
        if (topologyCommSold.hasThresholds()) {
            final Thresholds threshold = topologyCommSold.getThresholds();
            if (threshold.hasMax()) {
                final float maxThreshold = (float)((float)threshold.getMax() * scalingFactor);
                economyCommSoldSettings.setCapacityUpperBound(maxThreshold);
            }
            if (threshold.hasMin()) {
                final float minThreshold = (float)((float)threshold.getMin() * scalingFactor);
                economyCommSoldSettings.setCapacityLowerBound(minThreshold);
            }

            logger.debug("Thresholds for {} of entity {} is Max: {} min: {}",
                comName, dto.getDisplayName(), economyCommSoldSettings.getCapacityUpperBound(),
                economyCommSoldSettings.getCapacityLowerBound());
        }

        // Set resold flag
        if (topologyCommSold.hasIsResold()) {
            economyCommSoldSettings.setResold(topologyCommSold.getIsResold());
        }

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
        maxQuantityFloat *= scalingFactor;
        // if entry not present, initialize to 0
        int numConsumers = Optional.ofNullable(numConsumersOfSoldCommTable.get(dto.getOid(),
                topologyCommSold.getCommodityType())).map(o -> o.intValue()).orElse(0);
        float peak = getUsedValue(topologyCommSold, TopologyDTO.CommoditySoldDTO::getPeak,
                                  topologyCommSold.hasHistoricalPeak()
                                                  ? TopologyDTO.CommoditySoldDTO::getHistoricalPeak
                                                  : null);
        peak *= scalingFactor;

        // Subtract peak value of consumers from peak value of provider.
        // This can only happen in a plan with entities to remove.
        if (shouldSubtractUsageAndPeak) {
            peak -= providerUsedSubtractionMap.get(dto.getOid()).get(topologyCommSold.getCommodityType()).peak;
        }

        if (peak < used) {
            peak = used;
        }
        int slots = (topologyCommSold.hasHistoricalUsed() &&
                topologyCommSold.getHistoricalUsed().getTimeSlotCount() > 0) ?
                    topologyCommSold.getHistoricalUsed().getTimeSlotCount() : 1;
        final Collection<CommoditySpecificationTO> commoditySpecs =
                commodityTypeAllocator.commoditySpecification(commodityType, slots);
        List<CommodityDTOs.CommoditySoldTO> soldCommodityTOs = new ArrayList<>(slots);
        /* we currently do not support timeslot analysis for sold commodities
         so we duplicate the realtime usage instead.*/
        for (CommoditySpecificationTO commoditySpec : commoditySpecs) {
            final Builder soldCommBuilder = CommoditySoldTO.newBuilder();
            soldCommBuilder.setPeakQuantity(peak)
                    .setCapacity(capacity)
                    .setQuantity(used)
                    // Warning: we are down casting from double to float.
                    // Market has to change this field to double
                    .setMaxQuantity(maxQuantityFloat)
                    .setSettings(economyCommSoldSettings)
                    .setSpecification(commoditySpec)
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
                soldCommBuilder.setHistoricalQuantity((float)((float)topologyCommSold.getCapacity()
                        * topologyCommSold.getScalingFactor()
                        * topologyCommSold.getHistoricalUsed().getPercentile()));
            }
            soldCommodityTOs.add(soldCommBuilder.build());
        }
        logger.debug("Created {} sold commodity TOs for {}",
                soldCommodityTOs.size(), topologyCommSold);
        return soldCommodityTOs;
    }

    /**
     * Determine if should subtract used and peak from the provider utilization when removing vms from
     * plan.
     *
     * @param dto the topology entity
     * @param topologyCommSold the comm sold
     * @return boolean if should use the subtraction map
     */
    private boolean shouldUseSubtractionMap(TopologyEntityDTO dto, CommoditySoldDTO topologyCommSold) {
        return providerUsedSubtractionMap != null
                && providerUsedSubtractionMap.containsKey(dto.getOid())
                && providerUsedSubtractionMap.get(dto.getOid())
                    .containsKey(topologyCommSold.getCommodityType());
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

        Collection<CommodityDTOs.CommoditySoldTO> soldTOs =
                createCommoditySoldTO(commodityType, capacity, used, uf, 1);

        if (soldTOs.size() > 1) {
            logger.warn("Unexpected number of sold commodities(={}) for {}",
                    soldTOs.size(), commodityType);
        }
        return soldTOs.iterator().next();
    }

    /**
     * Creates a {@link CommoditySoldTO} of specific type with a specific used and capacity.
     *
     * @param commodityType {@link CommodityType} of commodity to be created
     * @param capacity is the capacity of the commSold
     * @param used is the current used of the commSold
     * @param uf is the updating function of the commold
     * @param  slots the number of slots for this entity set in analysis settings
     * @return a list of {@link CommoditySoldTO}
     */
    @Nonnull
    public Collection<CommoditySoldTO> createCommoditySoldTO(
            @Nonnull CommodityType commodityType,
            float capacity,
            float used, @Nonnull UpdatingFunctionTO uf, int slots) {
        final CommodityDTOs.CommoditySoldSettingsTO economyCommSoldSettings =
                CommodityDTOs.CommoditySoldSettingsTO.newBuilder()
                        .setResizable(false)
                        .setPriceFunction(priceFunction(commodityType, 1.0f, null, 1, enableOP))
                        .setUpdateFunction(uf)
                        .build();
        final Collection<CommoditySpecificationTO> commoditySpecs =
                commodityTypeAllocator.commoditySpecification(commodityType, slots);
        List<CommodityDTOs.CommoditySoldTO> soldCommodityTOs = new ArrayList<>();
        for (CommoditySpecificationTO commoditySpec : commoditySpecs) {
            soldCommodityTOs.add(CommodityDTOs.CommoditySoldTO.newBuilder()
                    .setPeakQuantity(0)
                    .setCapacity(capacity)
                    .setQuantity(used)
                    // Warning: we are down casting from double to float.
                    // Market has to change this field to double
                    .setSettings(economyCommSoldSettings)
                    .setSpecification(commoditySpec)
                    .setThin(false)
                    .build());
        }
        return soldCommodityTOs;
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
     * Is {@link CommodityType} a biClique Commodity?
     *
     * @param commodityType the commodityType to be checked
     * @return True if the type is DSPM or DATASTORE commodity and it has a key. False otherwise.
     */
    public static boolean isBicliqueCommodity(CommodityType commodityType) {
        boolean isOfType = AnalysisUtil.DSPM_OR_DATASTORE.contains(commodityType.getType());
        return isOfType && commodityType.hasKey();
    }

    @Nonnull
    public Optional<CommodityType> marketToTopologyCommodity(
            @Nonnull final CommodityDTOs.CommoditySpecificationTO marketCommodity) {
        return commodityTypeAllocator.marketToTopologyCommodity(marketCommodity);
    }

    @VisibleForTesting
    @Nonnull
    Optional<CommodityType> marketToTopologyCommodity(
            @Nonnull final CommodityDTOs.CommoditySpecificationTO marketCommodity,
            final Optional<Integer> slotIndex) {
        return commodityTypeAllocator.marketToTopologyCommodity(marketCommodity, slotIndex);
    }

    /**
     * Check if commodity for the specified market spec is timeslot commodity.
     *
     * @param marketCommodity Market commodity spec
     * @return True if timeslot commodity
     */
    public boolean isTimeSlotCommodity(final CommoditySpecificationTO marketCommodity) {
        return commodityTypeAllocator.isTimeSlotCommodity(marketCommodity);
    }

    /**
     * Constructs a string that can be used for debug purposes.
     * @param commType the description of a commodity
     * @return a string in the format "VCPU|P1" when the specification includes a non-empty key
     * and just "VCPU" otherwise.
     */
    @Nonnull
    public static String commodityDebugInfo(
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
     * @param dto the entity whose commodity price function is being set.
     * @param additionalSoldWeight is the weight for soldCommodities.
     * @param enableOP flag to check if to use over provisioning commodity changes.
     * @return a (reusable) instance of PriceFunctionTO to use in the commodity sold settings.
     */
    @Nonnull
    private static PriceFunctionDTOs.PriceFunctionTO priceFunction(CommodityType commType,
                                                                   float scale,
                                                                   TopologyEntityDTO dto,
                                                                   float additionalSoldWeight,
                                                                   boolean enableOP) {
        // logic to choose correct price function is based on commodity type
        return MarketAnalysisUtils.priceFunction(commType, scale, dto, additionalSoldWeight, enableOP);
    }

    /**
     * Select the right {@link UpdatingFunctionTO} based on the commodity sold type.
     *
     * @param topologyCommSold       a commodity sold for which to add an updating function
     * @param commodityTypeAllocator a commodity type allocator
     * @param topologyInfo           the current topology info
     * @return a (reusable) instance of UpdatingFunctionTO to use in the commodity sold settings.
     */
    @Nonnull
    private static UpdatingFunctionTO
                    updateFunction(final CommoditySoldDTO topologyCommSold,
                                   final CommodityTypeAllocator commodityTypeAllocator,
                                   final TopologyInfo topologyInfo) {
        return MarketAnalysisUtils.updateFunction(topologyCommSold.getCommodityType(),
                commodityTypeAllocator, topologyInfo);
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
                .setSpecification(commodityTypeAllocator.commoditySpecificationBiClique(bcKey))
                .setSettings(MarketAnalysisUtils.BC_SETTING_TO)
                .build();
    }



   /**
     * Returns the historical value for sold and bought commodity with the percentile value given
     * a higher priority.
     * @param commDto the sold commodity or the bought commodity.
     * @param historicalExtractor function that extracts the historical utilization(used) value.
     * @return AN optional of the the array of historical used/peak values, returns empty
    * if historical value is not set.
     */
   @Nonnull
   public static Optional<float[]> getHistoricalUsedOrPeak(final CommodityBoughtDTO commDto,
                    @Nonnull final Function<CommodityBoughtDTO, HistoricalValues> historicalExtractor) {
        final HistoricalValues hv = historicalExtractor.apply(commDto);
        if (hv.hasPercentile()) {
            final float value = (float)hv.getPercentile();
            logger.debug("Using percentile value {} for recalculating resize capacity for {}",
                value, commDto.getCommodityType().getType());
            return Optional.of(new float[]{Float.valueOf(value)});
        } else if (hv.getTimeSlotCount() > 1 &&
            TIMESLOT_COMMODITIES.contains(commDto.getCommodityType().getType())) {
            final float[] timeslotValueArr = ArrayUtils.toPrimitive(hv.getTimeSlotList().stream()
                .filter(Objects::nonNull)
                .map(x -> x.floatValue())
                .collect(Collectors.toList())
                .toArray(new Float[]{}));
            logger.debug("Using time slot values {} for recalculating resize capacity for {}",
                timeslotValueArr, commDto.getCommodityType().getType());
            return Optional.of(timeslotValueArr);
        } else if (hv.hasHistUtilization()) {
            // if not then hist utilization which is the historical used value.
            final float value = (float)hv.getHistUtilization();
            logger.debug("Using hist Utilization value {} for recalculating resize capacity for {}",
                value, commDto.getCommodityType().getType());
            return Optional.of(new float[]{Float.valueOf(value)});
        } else {
            return Optional.empty();
        }
   }

    /**
     * Fetch the single used or peak value for a topology commodity.
     * System load > history utilization > real-time usage.
     *
     * @param <DtoTypeT> sold or bo237ught topology commodity dto type
     * @param commDto sold or bought commodity dto
     * @param usedExtractor how to get real-time usage from dto
     * @param historicalExtractor how to get history usage from dto, null if dto has no history values
     * @return non-null value (ultimately there are 0 defaults)
     */
    public static <DtoTypeT> float
           getUsedValue(@Nonnull DtoTypeT commDto,
                        @Nonnull Function<DtoTypeT, Double> usedExtractor,
                        @Nullable Function<DtoTypeT, HistoricalValues> historicalExtractor) {
        if (historicalExtractor != null) {
            HistoricalValues hv = historicalExtractor.apply(commDto);
            if (hv.hasMovingMeanPlusStandardDeviations()) {
                // if present then return moving mean with stdDeviation
                return (float)hv.getMovingMeanPlusStandardDeviations();
            }
            if (hv.hasHistUtilization()) {
                // if present then hist utilization
                return (float)hv.getHistUtilization();
            }
        }
        // otherwise take real-time 'used'
        // real-time values have 0 defaults so there cannot be nulls
        return usedExtractor.apply(commDto).floatValue();
    }

    /**
     * Creates a {@link CommoditySpecificationTO} from a {@link CommodityType} and populates
     * the commoditySpecMap with commSpecTO to CommodityType mapping. Returns a list of specs
     * when the historical usage consists of utilizations split across observation periods.
     *
     * @param topologyCommodity the CommodityType for which the CommSpecTO is to be created
     * @param numberOfSlots the number of slots set in the entity's analysis settings
     * @return a list of {@link CommoditySpecificationTO} for the {@link CommodityType}
     */
    @Nonnull
    public Collection<CommoditySpecificationTO> commoditySpecification(
            @Nonnull final CommodityType topologyCommodity,
            final int numberOfSlots) {
        return commodityTypeAllocator.commoditySpecification(topologyCommodity, numberOfSlots);
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
        Collection<CommoditySpecificationTO> specs =
                commodityTypeAllocator.commoditySpecification(topologyCommodity, 1);
        if (specs.size() > 1) {
            logger.warn("Multiple specs obtained for {}", topologyCommodity);
        }
        return specs.iterator().next();
    }

    /**
     * Retrieve commodity type for the specified market commodity ID.
     * @param marketCommodityId Market commodity ID
     * @return {@link CommodityType}
     */
    @VisibleForTesting
    @Nullable
    public CommodityType commodityIdToCommodityType(final int marketCommodityId) {
        return commodityTypeAllocator.marketCommIdToCommodityType(marketCommodityId);
    }

    /**
     * Retrieve commodity type and slot number for the specified market commodity ID.
     *
     * @param marketCommodityId Market commodity ID
     * @return Pair containing {@link CommodityType} and slot number for time slot commodities
     *              or empty Optional for non-time slot commodities
     */
    @Nonnull
    Pair<CommodityType, Optional<Integer>> commodityIdToCommodityTypeAndSlot(
        final int marketCommodityId) {
        return commodityTypeAllocator.marketCommIdToCommodityTypeAndSlot(marketCommodityId);
    }

    /**
     * Gets the name of the commodity from the id.
     *
     * @param marketCommodityId the commodity id for which the name is needed
     * @return the market name of the commodity
     */
    String getCommodityName(int marketCommodityId) {
        return commodityTypeAllocator.getMarketCommodityName(marketCommodityId);
    }

    /**
     * Create a CommSpecTO for a biclique key.
     *
     * @param bcKey the biClique key for which CommSpecTO is to be created
     * @return the {@link CommoditySpecificationTO} for the biClique key
     */
    @Nonnull
    public CommodityDTOs.CommoditySpecificationTO commoditySpecificationBiClique(@Nonnull String bcKey) {
        return commodityTypeAllocator.commoditySpecificationBiClique(bcKey);
    }

    /**
     * utility method to check if a market id is that of a biclique.
     * @param marketId the market id to check
     * @return true if the commodity is a BiClique commodity
     */
    public boolean isSpecBiClique(final int marketId) {
        return marketId == CommodityDTO.CommodityType.BICLIQUE_VALUE;
    }

    /**
     * Set the providerUsedSubtractionMap.
     *
     * @param providerUsedSubtractionMap providerUsedSubtractionMap
     */
    void setProviderUsedSubtractionMap(final Map<Long, Map<CommodityType, UsedAndPeak>> providerUsedSubtractionMap) {
        this.providerUsedSubtractionMap = providerUsedSubtractionMap;
    }

    Map<Long, Map<CommodityType, UsedAndPeak>> getProviderUsedSubtractionMap() {
        return this.providerUsedSubtractionMap;
    }

    /**
     * Clear the providerUsedSubtractionMap.
     */
    void clearProviderUsedSubtractionMap() {
        this.providerUsedSubtractionMap = Collections.emptyMap();
    }
}
