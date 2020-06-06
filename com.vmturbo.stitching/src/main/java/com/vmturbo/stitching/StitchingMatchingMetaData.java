package com.vmturbo.stitching;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;

/**
 * A class that encapsulates the meta data that is needed to stitch a particular entity type for
 * a probe.
 *
 * @param <INTERNAL_SIGNATURE_TYPE> The type of the matching signature on the probe side.
 * @param <EXTERNAL_SIGNATURE_TYPE> The type of the matching signature on the server side.
 */
public interface StitchingMatchingMetaData<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE> {
    // the type of entity to match on on the probe side.
    EntityType getInternalEntityType();

    /**
     * A collection that captures the properties or fields to match on.  In most cases this will be
     * a single property or field.  A collection can be used for String type {@link
     * MatchingPropertyOrField}s only where all values are concatenated together to get the matching
     * value. The type of entity to match on the server side.
     *
     * @return collection that captures the properties or fields to match on.
     */
    @Nonnull
    Collection<MatchingPropertyOrField<INTERNAL_SIGNATURE_TYPE>> getInternalMatchingData();
    EntityType getExternalEntityType();

    /**
     * A collection that captures the properties or fields to match on.  In most cases this will be
     * a single property or field.  A collection can be used for String type {@link
     * MatchingPropertyOrField}s only where all values are concatenated together to get the matching
     * value.
     *
     * @return collection that captures the properties or fields to match on.
     */
    @Nonnull
    Collection<MatchingPropertyOrField<EXTERNAL_SIGNATURE_TYPE>> getExternalMatchingData();
    // a list of entity properties to patch
    Collection<String> getPropertiesToPatch();
    // a list of fields of the entity to patch
    Collection<DTOFieldSpec> getAttributesToPatch();
    // a list of commodities sold to patch
    Collection<CommoditySoldMetadata> getCommoditiesSoldToPatch();
    // a list of commodities bought to patch along with their providers
    Collection<CommodityBoughtMetadata> getCommoditiesBoughtToPatch();
    // whether or not to keep probe side entities that do not match
    boolean getKeepStandalone();
}
