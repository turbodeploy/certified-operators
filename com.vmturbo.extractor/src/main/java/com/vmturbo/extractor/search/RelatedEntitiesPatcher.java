package com.vmturbo.extractor.search;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add related entities info.
 */
public class RelatedEntitiesPatcher implements EntityRecordPatcher<DataProvider> {

    @Override
    public void patch(PartialRecordInfo recordInfo, DataProvider dataProvider) {
        final List<SearchMetadataMapping> metadataMappings = SearchMetadataUtils.getMetadata(
                recordInfo.entityType, FieldType.RELATED_ENTITY);
        // collect all related names first for use by all cases below
        final Map<EntityType, List<String>> relatedEntityNamesByType = metadataMappings.stream()
                .map(SearchMetadataMapping::getRelatedEntityTypes)
                .filter(Objects::nonNull)
                .flatMap(Set::stream)
                .collect(Collectors.toMap(relatedEntityType -> relatedEntityType,
                        relatedEntityType -> dataProvider.getRelatedEntityNames(recordInfo.oid,
                                EnumUtils.entityTypeFromApiToProto(relatedEntityType))));
        metadataMappings.forEach(metadata -> {
            List<String> relatedEntityNames = metadata.getRelatedEntityTypes().stream()
                    .flatMap(relatedEntityType -> relatedEntityNamesByType.getOrDefault(
                            relatedEntityType, Collections.emptyList()).stream())
                    .collect(Collectors.toList());
            switch (metadata.getRelatedEntityProperty()) {
                case NAMES:
                    recordInfo.attrs.put(metadata.getJsonKeyName(), relatedEntityNames);
                    break;
                case COUNT:
                    recordInfo.attrs.put(metadata.getJsonKeyName(), relatedEntityNames.size());
                    break;
            }
        });
    }
}
