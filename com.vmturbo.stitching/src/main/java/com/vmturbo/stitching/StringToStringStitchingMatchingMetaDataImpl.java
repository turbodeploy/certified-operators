package com.vmturbo.stitching;

import java.util.List;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;

/**
 * A class for encapsulating the meta data needed to stitch two entities when the both entities
 * return a String for matching.
 */
public class StringToStringStitchingMatchingMetaDataImpl extends
        StitchingMatchingMetaDataImpl<String, String> {

    public StringToStringStitchingMatchingMetaDataImpl(EntityType entityType,
                                                       MergedEntityMetadata mergeMetaData) {
        super(entityType, mergeMetaData);

    }

    @Override
    public List<MatchingPropertyOrField<String>> getInternalMatchingData() {
        return handleStringMatchingData(
                mergedEntityMetadata.getMatchingMetadata().getMatchingDataList());
    }

    @Override
    public List<MatchingPropertyOrField<String>> getExternalMatchingData() {
        return handleStringMatchingData(
                mergedEntityMetadata.getMatchingMetadata().getExternalEntityMatchingPropertyList());
    }
}
