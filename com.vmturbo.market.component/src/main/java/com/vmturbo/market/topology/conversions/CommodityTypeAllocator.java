package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * A wrapper class encapsulating the NumericIDAllocator for building
 * the commodity specifications and register allocated commodity types for reuse.
 */
public class CommodityTypeAllocator {

    private static final Logger logger = LogManager.getLogger();

    private final NumericIDAllocator idAllocator;
    private final CommodityIDKeyGenerator timeSlotCommodityIDKeyGenerator
            = new TimeSlotCommodityIDKeyGenerator();
    private final CommodityIDKeyGenerator defaultKeyGenerator
            = new DefaultCommodityIDKeyGenerator();
    private int bcBaseType = -1;

    // Mapping of CommoditySpecificationTO (string representation of type and baseType from
    // CommoditySpecificationTO) to specific CommodityType.
    private final Map<String, CommodityType>
            commoditySpecMap = Maps.newHashMap();

    CommodityTypeAllocator(final NumericIDAllocator commodityTypeAllocator) {
        this.idAllocator = commodityTypeAllocator;
    }

    /**
     * Create a CommSpecTO for a biclique key.
     *
     * @param bcKey the biClique key for which CommSpecTO is to be created
     * @return the {@link CommoditySpecificationTO} for the biClique key
     */
    @Nonnull
    public CommodityDTOs.CommoditySpecificationTO commoditySpecificationBiClique(@Nonnull String bcKey) {
        return CommodityDTOs.CommoditySpecificationTO.newBuilder()
                .setBaseType(bcBaseType())
                .setType(idAllocator.allocate(
                        CommodityDTO.CommodityType.BICLIQUE_VALUE
                                + TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR + bcKey))
                .setDebugInfoNeverUseInCode(TopologyConversionConstants.BICLIQUE + " " + bcKey)
                .build();
    }

    /**
     * The base type of bicliques. Allocates a new type if not already allocated.
     * @return An integer representing the base type of biCliques.
     */
    private int bcBaseType() {
        if (bcBaseType == -1) {
            bcBaseType = idAllocator.allocate(TopologyConversionConstants.BICLIQUE);
        }
        return bcBaseType;
    }

    /**
     * Creates a {@link CommoditySpecificationTO} from a {@link CommodityType} and populates
     * the commoditySpecMap with commSpecTO to CommodityType mapping.
     *
     * @param topologyCommodityType the CommodityType for which the CommSpecTO is to be created
     * @param numberOfSlots the number of slots set in the entity's analysis settings
     * @return the {@link CommoditySpecificationTO} for the {@link CommodityType}
     */
    @Nonnull
    public Collection<CommoditySpecificationTO> commoditySpecification(
            @Nonnull final CommodityType topologyCommodityType,
            final int numberOfSlots) {
        CommodityIDKeyGenerator idKeyGenerator =
                retrieveIdKeyGenerator(topologyCommodityType.getType());

        List<CommodityDTOs.CommoditySpecificationTO> specs = new ArrayList<>();
        for (int i = 0; i < numberOfSlots; i++) {

            final CommodityDTOs.CommoditySpecificationTO economyCommodity =
                    CommodityDTOs.CommoditySpecificationTO.newBuilder()
                            .setType(topologyToMarketCommodityId(topologyCommodityType, Optional.of(i)))
                            .setBaseType(topologyCommodityType.getType())
                            .setDebugInfoNeverUseInCode(CommodityConverter.commodityDebugInfo(topologyCommodityType))
                            .setCloneWithNewType(MarketAnalysisUtils.CLONE_COMMODITIES_WITH_NEW_TYPE
                                    .contains(topologyCommodityType.getType()))
                            .build();
            commoditySpecMap.put(
                    idKeyGenerator.getKeyFromCommoditySpecification(economyCommodity, Optional.of(i)),
                    topologyCommodityType);
            logger.debug("Added commodity spec {} for {}",
                    economyCommodity.toString(), topologyCommodityType);
            specs.add(economyCommodity);
        }
        return specs;
    }

    @Nonnull
    private CommodityIDKeyGenerator retrieveIdKeyGenerator(final int topologyCommodityType) {
        if (TopologyConversionConstants.TIMESLOT_COMMODITIES.contains(topologyCommodityType)) {
            return timeSlotCommodityIDKeyGenerator;
        }
        return defaultKeyGenerator;
    }


    @VisibleForTesting
    @Nonnull
    Optional<CommodityType> marketToTopologyCommodity(
            @Nonnull final CommodityDTOs.CommoditySpecificationTO marketCommodity,
            Optional<Integer> slotIndex) {

        CommodityIDKeyGenerator idKeyGenerator =
                retrieveIdKeyGenerator(marketCommodity.getBaseType());
        final CommodityType topologyCommodity =
                commoditySpecMap.get(idKeyGenerator.getKeyFromCommoditySpecification(marketCommodity,
                        slotIndex));
        if (topologyCommodity == null) {
            if (!TopologyConversionConstants.BICLIQUE.equals(idAllocator.getName(marketCommodity.getBaseType())
                    )) {
                // this is a biclique commodity
                logger.error("Market returned invalid commodity specification " +
                        marketCommodity + "! " +
                        "Registered ones are " + commoditySpecMap.keySet());
            }
            return Optional.empty();
        }
        return Optional.of(topologyCommodity);
    }

    @VisibleForTesting
    @Nonnull
    Optional<CommodityType> marketToTopologyCommodity(
            @Nonnull final CommodityDTOs.CommoditySpecificationTO marketCommodity) {
        return marketToTopologyCommodity(marketCommodity, Optional.empty());
    }

    @VisibleForTesting
    @Nonnull
    CommodityType marketCommIdToCommodityType(final int marketCommodityId) {
        return defaultKeyGenerator.stringToCommodityType(getMarketCommodityName(marketCommodityId));
    }

    /**
     * Uses a {@link NumericIDAllocator} to construct an integer type to
     * each unique combination of numeric commodity type + string key.
     * @param commType a commodity description that contains the numeric type and the key
     * @param slotIndex the slot index for the commodity if applicable
     * @return and integer identifying the type
     */
    private int topologyToMarketCommodityId(@Nonnull final CommodityType commType,
                                            final Optional<Integer> slotIndex) {
        CommodityIDKeyGenerator keyGenerator = retrieveIdKeyGenerator(commType.getType());
        String commodityTypeString = keyGenerator.commodityTypeToString(commType, slotIndex);
        return idAllocator.allocate(commodityTypeString);
    }

    /**
     * Gets the name of the commodity from the id.
     *
     * @param marketCommId the commodity id for which the name is needed
     * @return the name of the commodity
     */
    public String getMarketCommodityName(int marketCommId) {
        return idAllocator.getName(marketCommId);
    }

    /**
     * utility method to check if a market id is that of a biclique.
     * @param marketId the market id to check
     * @return true if the id is the one assigned to TopologyConversionConstants.BICLIQUE
     */
    public boolean isSpecBiClique(final int marketId) {
        return (marketId == bcBaseType());
    }


    /**
     * Generates the Identity key for a commodity type is built.
     * Also provides for the parsing of identity keys.
     */
    private interface CommodityIDKeyGenerator {


        /**
         * Given the type string, this will parse the key and return the CommodityType.
         * @param commodityTypeString the string format of commodityType
         * @return The CommodityType parsed from the string
         */
        @Nullable
        CommodityType stringToCommodityType(@Nonnull String commodityTypeString);

        /**
         * Return specific key based on type and base type of CommoditySpecificationTO.
         * @param marketCommodity to generate key from.
         * @param slotIndex the slot index for the commodity if applicable
         * @return generated key.
         */
        @Nullable
        String getKeyFromCommoditySpecification(
                @Nonnull CommodityDTOs.CommoditySpecificationTO marketCommodity,
                @Nonnull Optional<Integer> slotIndex
        );

        /**
         * Concatenates the type and the key of the {@link CommodityType}.
         *
         * @param commType the {@link CommodityType} for which string conversion is desired
         * @param  slotIndex the slot index if applicable
         * @return string conversion of {@link CommodityType}
         */
        @Nullable
        String commodityTypeToString(@Nonnull CommodityType commType,
                                     @Nonnull Optional<Integer> slotIndex);
    }

    /**
     * Default implementation of the CommodityIDKey Generator.
     */
    private static class DefaultCommodityIDKeyGenerator implements CommodityIDKeyGenerator {

        @Override
        @Nullable
        public CommodityType stringToCommodityType(@Nonnull final String commodityTypeString) {
            int separatorIndex = commodityTypeString.indexOf(TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR);
            try {
                if (separatorIndex > -1) {
                    int type = Integer.parseInt(commodityTypeString.substring(0, separatorIndex));
                    if (separatorIndex > 0) {
                        return CommodityType.newBuilder()
                                .setType(type)
                                .setKey(commodityTypeString.substring(separatorIndex + 1))
                                .build();
                    }
                }
                return CommodityType.newBuilder()
                            .setType(Integer.parseInt(commodityTypeString))
                            .build();
            } catch (NumberFormatException exception) {
                logger.error("Encountered number format exception parsing {}", commodityTypeString);
                return null;
            } catch ( StringIndexOutOfBoundsException exception) {
                logger.error("Encountered indexing exception parsing {}", commodityTypeString);
                return null;
            }
        }

        // TODO Would be overridden in TimeSlotCommodityIDKeyGenerator when converting
        // back to DTO
        @Override
        @Nonnull
        public String getKeyFromCommoditySpecification(@Nonnull final CommoditySpecificationTO economyCommodity,
                                                       @Nonnull final Optional<Integer> slotIndex) {
            return (economyCommodity.getType()) +
                    TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR +
                    (economyCommodity.getBaseType());
        }


        @Override
        @Nullable
        public String commodityTypeToString(@Nonnull final CommodityType commType,
                                            @Nonnull final Optional<Integer> slotIndex) {
            int type = commType.getType();
            return type + (commType.hasKey() ?
                    TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR + commType.getKey()
                    : "");
        }
    }

    /**
     * Commodity type allocator for time slot based commodities where the
     * the slot index is part of the commodity key.
     */
    private static class TimeSlotCommodityIDKeyGenerator extends DefaultCommodityIDKeyGenerator {


        /**
         * Concatenates the type and the key of the {@link CommodityType}.
         *
         * @param commType the {@link CommodityType} for which string conversion is desired
         * @param slotIndex index in the slot list
         * @return string conversion of {@link CommodityType}
         */
        @Override
        @Nullable
        public String commodityTypeToString(@Nonnull final CommodityType commType,
                                            @Nonnull final Optional<Integer> slotIndex) {
            if (!slotIndex.isPresent()) {
                logger.error("A non time slot commodity cannot have a slot index");
                return null;
            }
            int type = commType.getType();
            String index = String.valueOf(slotIndex.get());
            final String key = (commType.hasKey() ?
                    TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR + commType.getKey()
                    : "");
            return String.join(TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR,
                    ImmutableList.of(String.valueOf(type), key, index));
        }

    }


}
