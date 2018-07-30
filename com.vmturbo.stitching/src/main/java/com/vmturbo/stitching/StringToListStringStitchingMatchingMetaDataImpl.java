package com.vmturbo.stitching;

import java.util.List;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;


/**
 * A class for encapsulating the meta data needed to stitch two entities when the first returns
 * a String for matching and the second returns a List of Strings.
 */public class StringToListStringStitchingMatchingMetaDataImpl extends
        StitchingMatchingMetaDataImpl<String, List<String>> {

    public StringToListStringStitchingMatchingMetaDataImpl(EntityType entityType,
                                                           MergedEntityMetadata mergeMetaData) {
        super(entityType, mergeMetaData);

    }

    @Override
    public List<MatchingPropertyOrField<String>> getInternalMatchingData() {
        return handleStringMatchingData(
                mergedEntityMetadata.getMatchingMetadata().getMatchingDataList());
    }

    @Override
    public List<MatchingPropertyOrField<List<String>>> getExternalMatchingData() {
        return handleListStringMatchingData(
                mergedEntityMetadata.getMatchingMetadata().getExternalEntityMatchingPropertyList());
    }
}
