package com.vmturbo.extractor.patchers;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.api.dto.searchquery.MemberFieldApiDTO.Property;
import com.vmturbo.extractor.search.EnumUtils.SearchEntityTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialEntityInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add member field, like member count.
 */
public class GroupMemberFieldPatcher implements EntityRecordPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void fetch(PartialEntityInfo recordInfo, DataProvider dataProvider) {
        // find metadata for member fields
        final List<SearchMetadataMapping> metadataList =
                SearchMetadataUtils.getMetadata(recordInfo.getGroupType(), FieldType.MEMBER);

        final long groupId = recordInfo.getOid();

        metadataList.forEach(metadata -> {
            // only member count is handled now
            if (metadata.getMemberProperty() == Property.COUNT) {
                // handle direct/indirect members count
                patchMembersCount(metadata, groupId, recordInfo, dataProvider);
            } else {
                logger.error("Unsupported group member property: {}", metadata.getMemberProperty());
            }
        });
    }

    private void patchMembersCount(SearchMetadataMapping metadata, long groupId,
                    PartialEntityInfo recordInfo, DataProvider dataProvider) {
        if (metadata.isDirect()) {
            // direct members count
            if (metadata.getMemberType() == null) {
                // all direct members (used for all regular groups)
                recordInfo.putAttr(metadata, dataProvider.getGroupDirectMembersCount(groupId));
            } else {
                // direct members of specific type (not used for now)
                recordInfo.putAttr(metadata, dataProvider.getGroupDirectMembersCount(groupId,
                        SearchEntityTypeUtils.apiToProto(metadata.getMemberType())));
            }
        } else {
            // indirect members count
            if (metadata.getMemberType() == null) {
                // all indirect members (not used for now)
                recordInfo.putAttr(metadata, dataProvider.getGroupIndirectMembersCount(groupId));
            } else {
                // indirect members of specific type
                recordInfo.putAttr(metadata, dataProvider.getGroupIndirectMembersCount(groupId,
                        SearchEntityTypeUtils.apiToProto(metadata.getMemberType())));
            }
        }
    }
}
