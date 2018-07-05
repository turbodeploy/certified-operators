package com.vmturbo.stitching.utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Sets;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.CommodityBoughtMetaData;
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

        private Collection<CommodityBoughtMetaData> boughtMetaData;

        private CopyCommoditiesBoughtStart() {
            boughtMetaData = null;
        }

        private CopyCommoditiesBoughtStart(@Nonnull final Collection<CommodityBoughtMetaData>
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
        private final Collection<CommodityBoughtMetaData> boughtMetaData;

        CopyCommoditiesBoughtWithSource(@Nonnull final StitchingEntity source,
                                        @Nullable Collection<CommodityBoughtMetaData> boughtMetaData) {
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
        private final Collection<CommodityBoughtMetaData> boughtMetaData;

        CopyCommoditiesWithDestination(@Nonnull final StitchingEntity destination,
                                       @Nullable Collection<CommodityBoughtMetaData> boughtMetaData) {
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

    public static CopyCommoditiesBoughtStart copyCommodities(@Nonnull final Collection<CommodityBoughtMetaData>
                                                                     metaData) {
        return new CopyCommoditiesBoughtStart(metaData);
    }

    /**
     * Take a list of all commodities from the provider and pare it down to only those that are
     * specified by the boughtMetaData.
     *
     * @param provider Provider related to the CommoditiesBought.
     * @param commodities Set of commodities that are bought.
     * @param boughtMetaData Metadata specifying which commodities should be transferred from proxy.
     * @return set of commodities that should be transferred from proxy to real entity.
     */
    private static List<CommodityDTO.Builder> verifyCommoditiesBought(@Nonnull StitchingEntity provider,
                                           @Nonnull List<CommodityDTO.Builder> commodities,
                                           @Nullable Collection<CommodityBoughtMetaData> boughtMetaData) {
        if (boughtMetaData == null) {
            return commodities;
        }
        Optional<Collection<CommodityType>> commodityTransferList =
                boughtMetaData.stream().filter(b -> provider.getEntityType().equals(b.getProviderType()))
                .findAny().map(CommodityBoughtMetaData::getCommodities);
        if (commodityTransferList.isPresent()) {
            Set<CommodityType> keepCommodities = Sets.newHashSet(commodityTransferList.get());
            return commodities.stream().filter(comm -> keepCommodities.contains(comm.getCommodityType()))
                    .collect(Collectors.toList());

        }
        else return new ArrayList<>();
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
                                              @Nullable Collection<CommodityBoughtMetaData> boughtMetaData) {
        final Map<StitchingEntity, List<CommodityDTO.Builder>> destinationBought =
                destination.getCommoditiesBoughtByProvider();

        if (source.getCommoditiesBoughtByProvider().isEmpty()) {
            logger.warn("Attempting to copy commodities from {} when it has no commodities to copy. " +
                    "Was this entity already removed from the topology?", source);
        }

        source.getCommoditiesBoughtByProvider().forEach((provider, commoditiesBought) ->
                destinationBought.computeIfAbsent(provider, key -> new ArrayList<>())
                        .addAll(verifyCommoditiesBought(provider, commoditiesBought, boughtMetaData)));
    }
}
