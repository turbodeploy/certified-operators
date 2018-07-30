package com.vmturbo.stitching;

import java.util.List;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;

/**
 * A class for encapsulating the meta data needed to stitch two entities that both return
 * List String for their matching data.
 */
public class ListStringToListStringStitchingMatchingMetaDataImpl extends
        StitchingMatchingMetaDataImpl<List<String>, List<String>> {

    public ListStringToListStringStitchingMatchingMetaDataImpl(EntityType entityType,
                                                               MergedEntityMetadata mergeMetaData) {
        super(entityType, mergeMetaData);

    }

    @Override
    public List<MatchingPropertyOrField<List<String>>> getInternalMatchingData() {
        return handleListStringMatchingData(
                mergedEntityMetadata.getMatchingMetadata().getMatchingDataList());
    }

    @Override
    public List<MatchingPropertyOrField<List<String>>> getExternalMatchingData() {
        return handleListStringMatchingData(
                mergedEntityMetadata.getMatchingMetadata().getExternalEntityMatchingPropertyList());
    }
}
