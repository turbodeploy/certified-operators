package com.vmturbo.stitching;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;

/**
 * A class for encapsulating the meta data needed to stitch two entities that both return {@link
 * String} {@link Collection} for their matching data.
 */
public class StringsToStringsStitchingMatchingMetaData
                extends StitchingMatchingMetaDataImpl<String, String> {

    /**
     * Creates {@link StringsToStringsStitchingMatchingMetaData} instance.
     *
     * @param entityType entity type for which matching metadata will be created.
     * @param mergeMetaData contains information about fields and way of matching
     *                 property values extraction.
     */
    public StringsToStringsStitchingMatchingMetaData(@Nonnull EntityType entityType,
                    @Nonnull MergedEntityMetadata mergeMetaData) {
        super(entityType, mergeMetaData);
    }

    @Nonnull
    @Override
    public Collection<MatchingPropertyOrField<String>> getInternalMatchingData() {
        final MatchingMetadata matchingMetadata = mergedEntityMetadata.getMatchingMetadata();
        return handleStringsMatchingData(matchingMetadata.getMatchingDataList());
    }

    @Nonnull
    @Override
    public Collection<MatchingPropertyOrField<String>> getExternalMatchingData() {
        final MatchingMetadata matchingMetadata = mergedEntityMetadata.getMatchingMetadata();
        return handleStringsMatchingData(matchingMetadata.getExternalEntityMatchingPropertyList());
    }
}
