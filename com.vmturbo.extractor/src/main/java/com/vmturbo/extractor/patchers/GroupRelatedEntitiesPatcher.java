package com.vmturbo.extractor.patchers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.api.dto.searchquery.RelatedEntityFieldApiDTO.RelatedEntitiesProperty;
import com.vmturbo.extractor.search.EnumUtils.SearchEntityTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add relatedEntity field, like entityCounts.
 */
public class GroupRelatedEntitiesPatcher implements EntityRecordPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, DataProvider dataProvider) {
        // find metadata for relatedEntity fields
        final List<SearchMetadataMapping> metadataList =
                SearchMetadataUtils.getMetadata(recordInfo.getGroupType(), FieldType.RELATED_ENTITY);

        final Map<String, Object> attrs = recordInfo.getAttrs();
        final long groupId = recordInfo.getOid();

        metadataList.forEach(metadata -> {
            // only relatedEntity count is handled now
            if (metadata.getRelatedEntityProperty() == RelatedEntitiesProperty.COUNT) {
                patchRelatedEntityCount(metadata, groupId, attrs, dataProvider);
            } else {
                logger.error("Unsupported group realtedEntity property: {}", metadata.getRelatedEntityProperty());
            }
        });
    }

    private void patchRelatedEntityCount(SearchMetadataMapping metadata, long groupId,
            Map<String, Object> attrs, DataProvider dataProvider) {
        final String jsonKey = metadata.getJsonKeyName();

        if (metadata.getRelatedEntityTypes() != null) {
            // related entities count (vms count in a cluster)
            // only used by cluster for now
            Set<EntityType> relatedEntityTypes = metadata.getRelatedEntityTypes().stream()
                    .map(SearchEntityTypeUtils::apiToProto)
                    .collect(Collectors.toSet());
            attrs.put(jsonKey, dataProvider.getGroupRelatedEntitiesCount(
                    groupId, relatedEntityTypes));
        }
    }
}
