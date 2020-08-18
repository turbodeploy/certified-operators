package com.vmturbo.extractor.patchers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
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

        for (SearchMetadataMapping metadata : metadataList) {
            if (metadata.getRelatedEntityTypes() == null) {
                continue;
            }
            Set<EntityType> relatedEntityTypes = metadata.getRelatedEntityTypes().stream()
                    .map(SearchEntityTypeUtils::apiToProto)
                    .collect(Collectors.toSet());
            final String jsonKey = metadata.getJsonKeyName();

            switch (metadata.getRelatedEntityProperty()) {
                case NAMES:
                    attrs.put(jsonKey, dataProvider.getGroupRelatedEntitiesNames(
                            groupId, relatedEntityTypes));
                    break;
                case COUNT:
                    attrs.put(jsonKey, dataProvider.getGroupRelatedEntitiesCount(
                            groupId, relatedEntityTypes));
                    break;
                default:
                    logger.error("Unsupported group relatedEntity property: {}", metadata.getRelatedEntityProperty());
            }
        }
    }
}
