package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Pair;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.market.topology.TopologyConversionConstants;
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

    // Mapping of CommoditySpecificationTO (string representation of type and baseType from
    // CommoditySpecificationTO) to specific CommodityType.
    private final Map<Integer, CommodityType>
            commoditySpecMap = Maps.newHashMap();
    // Reuse commodity specifications based on (numeric) type.
    private final Map<Integer, CommoditySpecificationTO> reusableCommoditySpecs
            = Maps.newHashMap();

    // a set of commodity types that could be used as constraint in reservation
    private static final Set<Integer> reservationConstraintCommodities = Sets.newHashSet(
            CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE, CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE,
            CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE, CommodityDTO.CommodityType.CLUSTER_VALUE,
            CommodityDTO.CommodityType.DATACENTER_VALUE, CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE,
            CommodityDTO.CommodityType.NETWORK_VALUE, CommodityDTO.CommodityType.DRS_SEGMENTATION_VALUE,
            CommodityDTO.CommodityType.SEGMENTATION_VALUE);

    CommodityTypeAllocator(final NumericIDAllocator commodityTypeAllocator) {
        this.idAllocator = commodityTypeAllocator;
    }

    /**
     * Construct a map containing topologyDTO's {@link CommodityType} to economyDTO's
     * commoditySpecification type mapping. The map only contains reservation constraint
     * related commodity types mapping.
     *
     * @return a commodity type to integer map.
     */
    public Map<CommodityType, Integer> getReservationCommTypeToSpecMapping() {
        Map<CommodityType, Integer> commTypeToSpecMap = Maps.newHashMap();
        for (Map.Entry<Integer, CommodityType> e : commoditySpecMap.entrySet()) {
            Integer type = e.getKey();
            CommodityType topologyCommType = e.getValue();
            if (reservationConstraintCommodities.contains(topologyCommType.getType())) {
                commTypeToSpecMap.putIfAbsent(topologyCommType, type);
            }
        }
        return commTypeToSpecMap;
    }

    /**
     * Create a CommSpecTO for a biclique key.
     *
     * @param bcKey the biClique key for which CommSpecTO is to be created
     * @return the {@link CommoditySpecificationTO} for the biClique key
     */
    @Nonnull
    CommoditySpecificationTO commoditySpecificationBiClique(@Nonnull String bcKey) {
        return CommoditySpecificationTO.newBuilder()
                .setBaseType(CommodityDTO.CommodityType.BICLIQUE_VALUE)
                .setType(idAllocator.allocate(
                        CommodityDTO.CommodityType.BICLIQUE_VALUE
                                + TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR + bcKey))
                .setDebugInfoNeverUseInCode(CommodityDTO.CommodityType.BICLIQUE.toString() + " " + bcKey)
                .build();
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
        final CommodityIDKeyGenerator idKeyGenerator =
                retrieveIdKeyGenerator(topologyCommodityType.getType());

        final Collection<CommoditySpecificationTO> specs = new ArrayList<>(numberOfSlots);
        for (int i = 0; i < numberOfSlots; i++) {

            final String commodityTypeString =
                    idKeyGenerator.commodityTypeToString(topologyCommodityType, i);
            final int commodityType = idAllocator.allocate(commodityTypeString);
            final CommoditySpecificationTO economyCommodity =
                    reusableCommoditySpecs.computeIfAbsent(commodityType, newType ->
                        CommoditySpecificationTO.newBuilder()
                            .setType(newType)
                            .setBaseType(topologyCommodityType.getType())
                            .setDebugInfoNeverUseInCode(
                                    CommodityConverter.commodityDebugInfo(topologyCommodityType))
                            .setCloneWithNewType(MarketAnalysisUtils.CLONE_COMMODITIES_WITH_NEW_TYPE
                                    .contains(topologyCommodityType.getType()))
                            .build());
            commoditySpecMap.put(commodityType, topologyCommodityType);
            logger.debug("Added commodity spec {} for {}",
                economyCommodity::toString, () -> topologyCommodityType);
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
            @Nonnull final CommoditySpecificationTO marketCommodity,
            Optional<Integer> slotIndex) {
        // It's possible that type is equal to or greater than the size of idAllocator.
        // For example, the type of clone of certain commodity.
        int commodityType = marketCommodity.getType();
        if (commodityType >= idAllocator.size()) {
            return Optional.empty();
        }

        final CommodityType topologyCommodity = commoditySpecMap.get(commodityType);
        if (topologyCommodity == null) {
            if (marketCommodity.getBaseType() != CommodityDTO.CommodityType.BICLIQUE_VALUE) {
                // this is not a biclique commodity
                final String name = idAllocator.getName(commodityType);
                logger.error("Market commodity {} (baseType={}) registered in idAllocator for name '{}' does not have an entry in commoditySpecMap.",
                    commodityType, marketCommodity.getBaseType(), name);
            }
            return Optional.empty();
        }
        return Optional.of(topologyCommodity);
    }

    @VisibleForTesting
    @Nonnull
    Optional<CommodityType> marketToTopologyCommodity(
            @Nonnull final CommoditySpecificationTO marketCommodity) {
        return marketToTopologyCommodity(marketCommodity, Optional.empty());
    }

    /**
     * Retrieve commodity type for the specified market commodity ID.
     * @param marketCommodityId Market commodity ID
     * @return {@link CommodityType}
     */
    @VisibleForTesting
    @Nonnull
    CommodityType marketCommIdToCommodityType(final int marketCommodityId) {
        return defaultKeyGenerator.stringToCommodityType(getMarketCommodityName(marketCommodityId));
    }

    /**
     * Retrieve commodity type and slot number for the specified market commodity ID.
     *
     * @param marketCommodityId Market commodity ID
     * @return Pair containing {@link CommodityType} and slot number for time slot commodities
     *              or empty Optional for non-time slot commodities
     */
    @VisibleForTesting
    @Nonnull
    Pair<CommodityType, Optional<Integer>> marketCommIdToCommodityTypeAndSlot(final int marketCommodityId) {
        final String commodityTypeString = getMarketCommodityName(marketCommodityId);
        final CommodityType commodityType = defaultKeyGenerator.stringToCommodityType(
            commodityTypeString);
        final CommodityIDKeyGenerator keyGenerator = retrieveIdKeyGenerator(commodityType.getType());
        final Optional<Integer> slotNumber = keyGenerator.getCommoditySlotNumber(commodityTypeString);
        return new Pair<>(commodityType, slotNumber);
    }

    /**
     * Gets the name of the commodity from the id.
     *
     * @param marketCommId the commodity id for which the name is needed
     * @return the name of the commodity
     */
    String getMarketCommodityName(int marketCommId) {
        return idAllocator.getName(marketCommId);
    }

    /**
     * Check if commodity for the specified market spec is timeslot commodity.
     *
     * @param marketCommodity Market commodity spec
     * @return True if timeslot commodity
     */
    boolean isTimeSlotCommodity(final CommoditySpecificationTO marketCommodity) {
        if (MarketAnalysisUtils.CLONE_COMMODITIES_WITH_NEW_TYPE.contains(marketCommodity.getBaseType())) {
            return false;
        }

        final String commodityName = getMarketCommodityName(marketCommodity.getType());
        if (commodityName == null) {
            logger.error("Unknown commodity for market id {}", marketCommodity::getType);
            return false;
        }
        // timeslot commodity name is the pattern "baseType|key|slotNumber"
        return 2 < commodityName.split(Pattern.quote(
            TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR)).length;
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
                @Nonnull CommoditySpecificationTO marketCommodity,
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

        /**
         * Concatenates the type and the key of the {@link CommodityType}.
         *
         * @param commType the {@link CommodityType} for which string conversion is desired
         * @param  slotIndex the slot index if applicable
         * @return string conversion of {@link CommodityType}
         */
        @Nullable
        String commodityTypeToString(@Nonnull CommodityType commType, int slotIndex);

        /**
         * Get slot number for the specified commodity name.
         *
         *<p></p>
         * @param commodityTypeString Commodity type name, of the form baseType|key|slotnumber
         *              where key and slotnumebr are optional
         * @return Slot number ot empty Optional for non-time slot commodities
         */
        @Nonnull
        Optional<Integer> getCommoditySlotNumber(@Nonnull String commodityTypeString);
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
            return (economyCommodity.getType())
                    + TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR
                    + (economyCommodity.getBaseType());
        }

        @Override
        @Nullable
        public String commodityTypeToString(@Nonnull final CommodityType commType,
                                            @Nonnull final Optional<Integer> slotIndex) {
            // slotIndex is not used in default implementation
            return commodityTypeToString(commType, 0);
        }

        @Nullable
        @Override
        public String commodityTypeToString(@Nonnull final CommodityType commType, final int slotIndex) {
            int type = commType.getType();
            return type + (
                    commType.hasKey()
                    ? TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR + commType.getKey()
                    : "");
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public Optional<Integer> getCommoditySlotNumber(@Nonnull String commodityTypeString) {
            return Optional.empty();
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
                logger.error("Timeslot commodity {} must have a slot index", commType::getType);
                return null;
            }
            return commodityTypeToString(commType, slotIndex.get());
        }

        @Nullable
        @Override
        public String commodityTypeToString(@Nonnull final CommodityType commType, final int slotIndex) {
            final int type = commType.getType();
            final String key = (commType.hasKey() ? commType.getKey() : "");
            return String.join(TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR,
                ImmutableList.of(String.valueOf(type), key, String.valueOf(slotIndex)));
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public Optional<Integer> getCommoditySlotNumber(@Nonnull final String commodityTypeString) {
            // The expected commodity type is of the pattern: baseType|key|slotNumber
            // key is optional
            String[] parts = commodityTypeString.split(Pattern.quote(
                TopologyConversionConstants.COMMODITY_TYPE_KEY_SEPARATOR));
            if (3 > parts.length) {
                logger.error("Unexpectedly no slot number is present in commodity type {}",
                    () -> commodityTypeString);
                return Optional.empty();
            }
            final String slotNumber = parts[2];
            try {
                return Optional.of(Integer.valueOf(slotNumber));
            } catch (NumberFormatException e) {
                logger.error("Invalid slot number {} in commodity {}", () -> slotNumber,
                    () -> commodityTypeString);
                return Optional.empty();
            }
        }
    }
}
