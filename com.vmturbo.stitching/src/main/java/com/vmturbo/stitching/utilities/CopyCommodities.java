package com.vmturbo.stitching.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
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

        private CopyCommoditiesBoughtStart() {
        }

        public CopyCommoditiesWithDestination to(@Nonnull final StitchingEntity destination) {
            return new CopyCommoditiesWithDestination(destination);
        }

        public CopyCommoditiesBoughtWithSource from(@Nonnull final StitchingEntity source) {
            return new CopyCommoditiesBoughtWithSource(source);
        }
    }

    /**
     * A builder for a copy commodities operation that already includes the source entity for the copy.
     */
    public static class CopyCommoditiesBoughtWithSource {
        private final StitchingEntity source;

        CopyCommoditiesBoughtWithSource(@Nonnull final StitchingEntity source) {
            this.source = Objects.requireNonNull(source);
        }

        public void to(@Nonnull final StitchingEntity destination) {
            copyCommoditiesBought(source, destination);
        }
    }

    /**
     * A builder for a copy commodities operation that already includes the destination entity for the copy.
     */
    public static class CopyCommoditiesWithDestination {
        private final StitchingEntity destination;

        CopyCommoditiesWithDestination(@Nonnull final StitchingEntity destination) {
            this.destination = Objects.requireNonNull(destination);
        }

        public void from(@Nonnull final StitchingEntity source) {
            copyCommoditiesBought(source, destination);
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

    /**
     * Copy commodities bought from the source to the destination.
     *
     * For all providers of the new commodities bought, the destination is added as a consumer.
     *
     * @param source The source for the commodities copy.
     * @param destination The destination for the copy.
     */
    private static void copyCommoditiesBought(@Nonnull final StitchingEntity source,
                                              @Nonnull final StitchingEntity destination) {
        final Map<StitchingEntity, List<CommodityDTO.Builder>> destinationBought =
            destination.getCommoditiesBoughtByProvider();

        if (source.getCommoditiesBoughtByProvider().isEmpty()) {
            logger.warn("Attempting to copy commodities from {} when it has no commodities to copy. " +
                "Was this entity already removed from the topology?", source);
        }

        source.getCommoditiesBoughtByProvider().forEach((provider, commoditiesBought) ->
            destinationBought.computeIfAbsent(provider, key -> new ArrayList<>())
                .addAll(commoditiesBought));
    }
}
