package com.vmturbo.stitching;

import java.util.Collection;
import java.util.List;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

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
    // A list that captures the properties or fields to match on.  In most cases this will be a
    // single property or field.  A list can be used for String type MatchingOrPropertyFields only
    // where all values are concatenated together to get the matching String.
    List<MatchingPropertyOrField<INTERNAL_SIGNATURE_TYPE>> getInternalMatchingData();
    // the type of entity to match on the server side.
    EntityType getExternalEntityType();
    // A list that captures the properties or fields to match on.  In most cases this will be a
    // single property or field.  A list can be used for String type MatchingOrPropertyFields only
    // where all values are concatenated together to get the matching String.
    List <MatchingPropertyOrField<EXTERNAL_SIGNATURE_TYPE>> getExternalMatchingData();
    // a list of entity properties to patch
    Collection<String> getPropertiesToPatch();
    // a list of fields of the entity to patch
    Collection<DTOFieldSpec> getAttributesToPatch();
    // a list of commodities sold to patch
    Collection<CommodityType> getCommoditiesSoldToPatch();
    // a list of commodities bought to patch along with their providers
    Collection<CommodityBoughtMetaData> getCommoditiesBoughtToPatch();
    // whether or not to keep probe side entities that do not match
    boolean getKeepStandalone();
}
