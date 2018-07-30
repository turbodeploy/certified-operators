package com.vmturbo.stitching;

import java.util.List;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;

/**
 * A class for encapsulating the meta data needed to stitch two entities when the first returns
 * a List of Strings for matching and the second returns a String.
 */
public class ListStringToStringStitchingMatchingMetaDataImpl extends
        StitchingMatchingMetaDataImpl<List<String>, String> {

    public ListStringToStringStitchingMatchingMetaDataImpl(EntityType entityType,
                                                           MergedEntityMetadata mergeMetaData) {
        super(entityType, mergeMetaData);

    }

    @Override
    public List<MatchingPropertyOrField<List<String>>> getInternalMatchingData() {
        return handleListStringMatchingData(
                mergedEntityMetadata.getMatchingMetadata().getMatchingDataList());
    }

    @Override
    public List<MatchingPropertyOrField<String>> getExternalMatchingData() {
        return handleStringMatchingData(
                mergedEntityMetadata.getMatchingMetadata().getExternalEntityMatchingPropertyList());
    }
}