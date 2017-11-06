package com.vmturbo.stitching;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * A set of utilities that provides methods for performing common stitching updates.
 */
public class StitchingUpdateUtilities {

    /**
     * Prevent construction of this class because it only contains static utility methods.
     */
    private StitchingUpdateUtilities() {

    }

    /**
     * The copyType of commodities to be copied.
     */
    public enum CopyCommodityType {
        BOUGHT,
        SOLD,
        ALL
    }

    /**
     * A builder for allowing the copying of commodities from a source entity to a destination entity.
     */
    public static class CopyCommoditiesStart {
        private final CopyCommodityType copyType;

        private CopyCommoditiesStart(final CopyCommodityType copyType) {
            this.copyType = copyType;
        }

        public CopyCommoditiesWithDestination to(@Nonnull final EntityDTO.Builder destination) {
            return new CopyCommoditiesWithDestination(destination, copyType);
        }

        public CopyCommoditiesWithSource from(@Nonnull final EntityDTO.Builder source) {
            return new CopyCommoditiesWithSource(source, copyType);
        }
    }

    /**
     * A builder for a copy commodities operation that already includes the source entity for the copy.
     */
    public static class CopyCommoditiesWithSource {
        final EntityDTO.Builder source;
        final CopyCommodityType copyType;

        CopyCommoditiesWithSource(@Nonnull final EntityDTO.Builder source,
                                  final CopyCommodityType copyType) {
            this.source = Objects.requireNonNull(source);
            this.copyType = copyType;
        }

        public void to(@Nonnull final EntityDTO.Builder destination) {
            applyCopyCommodities(source, destination, copyType);
        }
    }

    /**
     * A builder for a copy commodities operation that already includes the destination entity for the copy.
     */
    public static class CopyCommoditiesWithDestination {
        final EntityDTO.Builder destination;
        final CopyCommodityType copyType;

        CopyCommoditiesWithDestination(@Nonnull final EntityDTO.Builder destination,
                                       final CopyCommodityType copyType) {
            this.destination = Objects.requireNonNull(destination);
            this.copyType = copyType;
        }

        public void from(@Nonnull final EntityDTO.Builder source) {
            applyCopyCommodities(source, destination, copyType);
        }
    }

    /**
     * A static helper method to create a builder for a copy commodities operation.
     *
     * Source and destination must be different entities.
     * Does not make any attempt to deduplicate or combine like commodities.
     * TODO: Support for commodity de-duplication and combining like commodities.
     *
     * @param copyType The type of copy that should be done.
     * @return A builder that permits specifying which commodities should be copied.
     */
    public static CopyCommoditiesStart copyCommodities(final CopyCommodityType copyType) {
        return new CopyCommoditiesStart(copyType);
    }

    /**
     * Apply a copy commodities operation by copying commodities from the source
     * to the destination.
     *
     * @param source The source entity from which the commodities should be copied.
     * @param destination The destination entity to which the commodities should be copied.
     * @param copyType The type of the copy that should be applied.
     */
    private static void applyCopyCommodities(@Nonnull final EntityDTO.Builder source,
                                             @Nonnull final EntityDTO.Builder destination,
                                             final CopyCommodityType copyType) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(destination);
        Preconditions.checkArgument(source != destination, "Source and destination entities cannot be the same");

        if (copyType != CopyCommodityType.SOLD) {
            copyBoughtCommodities(source, destination);
        }
        if (copyType != CopyCommodityType.BOUGHT) {
            copySoldCommodities(source, destination);
        }
    }

    private static void copyBoughtCommodities(@Nonnull final EntityDTO.Builder source,
                                              @Nonnull final EntityDTO.Builder destination) {
        source.getCommoditiesBoughtList().forEach(destination::addCommoditiesBought);
    }

    private static void copySoldCommodities(@Nonnull final EntityDTO.Builder source,
                                            @Nonnull final EntityDTO.Builder destination) {
        source.getCommoditiesSoldList().forEach(destination::addCommoditiesSold);
    }
}
