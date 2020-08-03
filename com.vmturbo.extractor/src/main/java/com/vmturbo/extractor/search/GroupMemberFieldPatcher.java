package com.vmturbo.extractor.search;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.api.dto.searchquery.MemberFieldApiDTO.Property;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add member field, like member count.
 */
public class GroupMemberFieldPatcher implements EntityRecordPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, DataProvider dataProvider) {
        // find metadata for member fields
        final List<SearchMetadataMapping> metadataList =
                SearchMetadataUtils.getMetadata(recordInfo.groupType, FieldType.MEMBER);

        final Map<String, Object> attrs = recordInfo.attrs;
        final long groupId = recordInfo.oid;

        metadataList.forEach(metadata -> {
            // only member count is handled now
            if (metadata.getMemberProperty() == Property.COUNT) {
                // handle direct/indirect members count
                patchMembersCount(metadata, groupId, attrs, dataProvider);
            } else {
                logger.error("Unsupported group member property: {}", metadata.getMemberProperty());
            }
        });
    }

    private void patchMembersCount(SearchMetadataMapping metadata, long groupId,
            Map<String, Object> attrs, DataProvider dataProvider) {
        final String jsonKey = metadata.getJsonKeyName();
        if (metadata.isDirect()) {
            // direct members count
            if (metadata.getMemberType() == null) {
                // all direct members (used for all regular groups)
                attrs.put(jsonKey, dataProvider.getGroupDirectMembersCount(groupId));
            } else {
                // direct members of specific type (not used for now)
                attrs.put(jsonKey, dataProvider.getGroupDirectMembersCount(groupId,
                        EnumUtils.entityTypeFromApiToProto(metadata.getMemberType())));
            }
        } else {
            // indirect members count
            if (metadata.getMemberType() == null) {
                // all indirect members (not used for now)
                attrs.put(jsonKey, dataProvider.getGroupIndirectMembersCount(groupId));
            } else {
                // indirect members of specific type
                attrs.put(jsonKey, dataProvider.getGroupIndirectMembersCount(groupId,
                        EnumUtils.entityTypeFromApiToProto(metadata.getMemberType())));
            }
        }
    }
}
