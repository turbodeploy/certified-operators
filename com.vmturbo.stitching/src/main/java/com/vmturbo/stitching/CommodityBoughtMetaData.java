package com.vmturbo.stitching;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A class that encapsulates information about Commodities bought by provider type that should
 * be stitched when we stitch two entities.
 */
public class CommodityBoughtMetaData {
    // EntityType of provider that is providing these commodities
    final private EntityType providerType;
    // Optional EntityType of external entity that this provider can replace
    final private Optional<EntityType> replacedEntityType;
    // Commodities bought from this provider that we want to pass to external entity during
    // stitching
    final private Collection<CommodityType> commodities;

    /**
     * Create a {@link CommodityBoughtMetaData}
     *
     * @param providerType - the {@link EntityType} of the provider of these commodities.
     * @param boughtCommodities - a collection of {@link CommodityType} defining the commodities
     *                          to push onto the stitched external entity when we stitch.
     */
    public CommodityBoughtMetaData(@Nonnull EntityType providerType,
                                   Collection<CommodityType> boughtCommodities) {
        this.providerType = providerType;
        this.commodities = boughtCommodities;
        replacedEntityType = Optional.empty();
    }

    /**
     *
     * @param providerType - the {@link EntityType} of the provider of these commodities.
     * @param replacedEntityType - the EntityType that this provider can replace when stitched.
     * @param boughtCommodities - a collection of {@link CommodityType} defining the commodities
     *                                to push onto the stitched external entity when we stitch.
     */
    public CommodityBoughtMetaData(@Nonnull EntityType providerType,
                                   @Nonnull EntityType replacedEntityType,
                                   Collection<CommodityType> boughtCommodities) {
        this.providerType = providerType;
        this.commodities = boughtCommodities;
        this.replacedEntityType = Optional.of(replacedEntityType);
    }

    /**
     * Get the {@link EntityType} of the provider.
     * @return the {@link EntityType} of the provider.
     */
    public EntityType getProviderType() {
        return providerType;
    }

    /**
     * Get the {@link EntityType} that this provider can replace, if there is one.
     * @return {@link Optional<EntityType>} representing the type, if any, that this provider can
     * replace.
     */
    public Optional<EntityType> getReplacedEntityType() {
        return replacedEntityType;
    }

    /**
     * Get the list of {@link CommodityType} that is bought from this provider and should be
     * stitched.
     *
     * @return {@link Collection<CommodityType>} for this provider.
     */
    public Collection<CommodityType> getCommodities() {
        return commodities;
    }
}
