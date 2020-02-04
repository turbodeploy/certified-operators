package com.vmturbo.stitching.utilities;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.platform.common.builders.CommodityBuilderIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.stitching.StitchingEntity;

/**
 * A set of utilities that provides methods for performing common stitching updates.
 */
public class CopyCommodities {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Prevent construction of this class because it only contains static utility methods.
     */
    private CopyCommodities() {

    }

    /**
     * A builder for allowing the copying of commodities from a source entity to a destination entity.
     */
    public static class CopyCommoditiesBoughtStart {

        private Collection<CommodityBoughtMetadata> boughtMetaData;

        private CopyCommoditiesBoughtStart() {
            boughtMetaData = null;
        }

        private CopyCommoditiesBoughtStart(@Nonnull final Collection<CommodityBoughtMetadata>
                                                   metaData) {
            boughtMetaData = metaData;
        }

        public CopyCommoditiesWithDestination to(@Nonnull final StitchingEntity destination) {
            return new CopyCommoditiesWithDestination(destination, boughtMetaData);
        }

        public CopyCommoditiesBoughtWithSource from(@Nonnull final StitchingEntity source) {
            return new CopyCommoditiesBoughtWithSource(source, boughtMetaData);
        }
    }

    /**
     * A builder for a copy commodities operation that already includes the source entity for the copy.
     */
    public static class CopyCommoditiesBoughtWithSource {
        private final StitchingEntity source;
        private final Collection<CommodityBoughtMetadata> boughtMetaData;

        CopyCommoditiesBoughtWithSource(@Nonnull final StitchingEntity source,
                                        @Nullable final Collection<CommodityBoughtMetadata>
                                                boughtMetaData) {
            this.source = Objects.requireNonNull(source);
            this.boughtMetaData = boughtMetaData;
        }

        public void to(@Nonnull final StitchingEntity destination) {
            copyCommoditiesBought(source, destination, boughtMetaData);
        }
    }

    /**
     * A builder for a copy commodities operation that already includes the destination entity for the copy.
     */
    public static class CopyCommoditiesWithDestination {
        private final StitchingEntity destination;
        private final Collection<CommodityBoughtMetadata> boughtMetaData;

        CopyCommoditiesWithDestination(@Nonnull final StitchingEntity destination,
                                       @Nullable Collection<CommodityBoughtMetadata> boughtMetaData) {
            this.destination = Objects.requireNonNull(destination);
            this.boughtMetaData = boughtMetaData;
        }

        public void from(@Nonnull final StitchingEntity source) {
            copyCommoditiesBought(source, destination, boughtMetaData);
        }
    }

    /**
     * A static helper method to create a builder for a copy commodities operation.
     *
     * Source and destination must be different entities.
     * Does not make any attempt to deduplicate or combine like commodities.
     * TODO: Support for commodity de-duplication and combining like commodities.
     *
     * @return A builder that permits specifying which commodities should be copied.
     */
    public static CopyCommoditiesBoughtStart copyCommodities() {
        return new CopyCommoditiesBoughtStart();
    }

    public static CopyCommoditiesBoughtStart copyCommodities(@Nonnull final Collection<CommodityBoughtMetadata>
                                                                     metaData) {
        return new CopyCommoditiesBoughtStart(metaData);
    }

    /**
     * Copy commodities bought from the source to the destination.
     * <p>
     * For all providers of the new commodities bought, the destination is added as a consumer.
     *
     * @param source      The source for the commodities copy.
     * @param destination The destination for the copy.
     */
    private static void copyCommoditiesBought(@Nonnull final StitchingEntity source,
                                              @Nonnull final StitchingEntity destination,
                                              @Nullable Collection<CommodityBoughtMetadata>
                                                      commBoughtMetaData) {
        final Map<StitchingEntity, List<CommoditiesBought>> destinationBought =
                destination.getCommodityBoughtListByProvider();

        if (source.getCommodityBoughtListByProvider().isEmpty()) {
            logger.warn("Attempting to copy commodities from {} when it has no commodities to copy. " +
                    "Was this entity already removed from the topology?", source);
        }

        // iterate over providers in the source and copy the commodities there over to destination
        // subject to the boughtMetaData Map.
        source.getCommodityBoughtListByProvider().forEach((provider, commodityBoughtList) -> {
            final Optional<Collection<CommodityType>> commodityMetaData =
                    commBoughtMetaData == null ? Optional.empty() : commBoughtMetaData.stream()
                            .filter(m -> m.getProviderType() == provider.getEntityType())
                            .findFirst()
                            .map(CommodityBoughtMetadata::getCommodityMetadataList);
            final List<CommoditiesBought> cbList = commodityBoughtList.stream()
                    .map(commodityBought -> {
                        Optional<CommoditiesBought> cb = destination.getMatchingCommoditiesBought(
                                provider, commodityBought);
                        List<CommodityDTO.Builder> commoditiesBought = mergeCommoditiesBought(
                                commodityBought.getBoughtList(),
                                cb.map(CommoditiesBought::getBoughtList).orElse(null),
                                commodityMetaData, commBoughtMetaData != null);
                        return new CommoditiesBought(commoditiesBought, commodityBought.getVolumeId());
                    }).collect(Collectors.toList());
            destinationBought.put(provider, cbList);
        });
    }

    /**
     * Copy the fromCommodities onto the ontoCommodities subject to the commodityMetaData.  If a
     * commodity attribute exists only in the onto commodity, keep its value.  If it exists in the
     * from commodity, overwrite the value in the onto commodity.
     *
     * @param fromCommodities CommodityDTO.Builders of commodities whose values we want to pass onto
     *                        the ontoCommodities
     * @param ontoCommodities CommodityDTO.Builders to receive the updated values
     * @param commodityMetaData List of CommodityTypes to transfer
     * @param filterFromCommodities true if we should filter fromCommodities by commodityMetaData,
     *                              false if not.  Should only be false for stitching operations
     *                              that don't specify commodityMetaData.
     * @return {@List<CommodityDTO.Builder>} giving the merged commodities.
     */
    public static List<CommodityDTO.Builder> mergeCommoditiesBought(
            @Nonnull final List<CommodityDTO.Builder> fromCommodities,
            @Nullable final List<CommodityDTO.Builder> ontoCommodities,
            @Nullable final Optional<Collection<CommodityType>> commodityMetaData,
            boolean filterFromCommodities) {
        List<CommodityDTO.Builder> retVal = Lists.newArrayList();
        // Collect the mergeFromCommodities into a map where they can be looked up by
        // {@link CommodityBuilderIdentifier}.
        final Map<CommodityBuilderIdentifier, CommodityDTO.Builder> mergeFromCommoditiesMap =
                fromCommodities.stream().collect(Collectors.toMap(
                        commodity -> new CommodityBuilderIdentifier(commodity.getCommodityType(),
                                commodity.getKey()), Function.identity()));

        // Collect the mergeOntoCommodities into a map where they can be looked up by
        // {@link CommodityBuilderIdentifier}.
        final Map<CommodityBuilderIdentifier, CommodityDTO.Builder> mergeOntoCommoditiesMap =
                ontoCommodities == null ? Maps.newHashMap() :
                        ontoCommodities.stream().collect(Collectors.toMap(
                                commodity -> new CommodityBuilderIdentifier(commodity.getCommodityType(),
                                        commodity.getKey()), Function.identity()));

        Set<CommodityType> mergeMetaDataSet = Sets.newHashSet();
        // if we are not filtering based on commodityMetaData, make sure all fromCommodities are
        // merged
        if (!filterFromCommodities) {
            for (CommodityDTO.Builder commBuilder : fromCommodities) {
                mergeMetaDataSet.add(commBuilder.getCommodityType());
            }
        } else {
            commodityMetaData.ifPresent(mergeMetaDataSet::addAll);
        }

        // For all from Commodities whose type exists in mergeMetaData, merge with matching onto
        // commodity if it exists, and then add to return value.
        for (Entry<CommodityBuilderIdentifier, Builder> entry :
                mergeFromCommoditiesMap.entrySet()) {
            if (mergeMetaDataSet.contains(entry.getKey().type)) {
                Builder ontoBuilder = mergeOntoCommoditiesMap.remove(entry.getKey());
                retVal.add(ontoBuilder == null ? entry.getValue()
                    : DTOFieldAndPropertyHandler.mergeBuilders(entry.getValue(), ontoBuilder,
                    // use empty list since we currently merge all fields for bought commodity
                    // we can specify fields to merge once there is a valid use case
                    Collections.emptyList()));
            }
        }
        // add any onto builders that didn't have matching from side commodities to the return value
        retVal.addAll(mergeOntoCommoditiesMap.values());
        return retVal;
    }
}
