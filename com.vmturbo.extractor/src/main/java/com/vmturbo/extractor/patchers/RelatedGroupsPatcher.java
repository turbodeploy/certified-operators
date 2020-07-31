package com.vmturbo.extractor.patchers;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.extractor.search.EnumUtils.GroupTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.SearchEntityTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add related groups info.
 *
 * <p>For example: If we have app1 (in appGroup1), vm1 (in vmGroup1), pm1 (in cluster1), and app1 is
 * hosted by vm1 which is hosted by pm1. For vm1, the related group can be vmGroup1, appGroup1 or
 * cluster1. The metadata tells us only cluster1 is what we want. So we will first expand vm1 to
 * related host, then find all groups which contains the host, and only choose the cluster type.
 * If we want to find related cluster for pm1, then we don't need to expand, since pm1 is same type
 * type as cluster member type, it will be able to find the cluster directly from the entityToGroup
 * map.</p>
 */
public class RelatedGroupsPatcher implements EntityRecordPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, DataProvider dataProvider) {
        final List<SearchMetadataMapping> metadataMappings = SearchMetadataUtils.getMetadata(
                recordInfo.getEntityType(), FieldType.RELATED_GROUP);

        metadataMappings.forEach(metadata -> {
            final GroupType relatedGroupType = GroupTypeUtils.apiToProto(
                    metadata.getRelatedGroupType());
            final EntityType relatedEntityType = SearchEntityTypeUtils.apiToProto(
                    metadata.getMemberType());
            // if related entity type is same as current entity type, no need to get related entity
            // like: find cluster of a host, we don't need to find related hosts of the host before
            // finding groups which contain it
            Set<Long> relatedEntities = recordInfo.getEntityType() == relatedEntityType.getNumber()
                    ? Collections.singleton(recordInfo.getOid())
                    : dataProvider.getRelatedEntitiesOfType(recordInfo.getOid(), relatedEntityType);
            Stream<Grouping> relatedGroups = relatedEntities.stream()
                    .map(dataProvider::getGroupsForEntity)
                    .flatMap(List::stream)
                    .filter(g -> g.getDefinition().getType() == relatedGroupType)
                    .distinct();

            switch (metadata.getRelatedGroupProperty()) {
                case COUNT:
                    recordInfo.putAttrs(metadata.getJsonKeyName(), relatedGroups.count());
                    break;
                case NAMES:
                    recordInfo.putAttrs(metadata.getJsonKeyName(),
                            relatedGroups.map(g -> g.getDefinition().getDisplayName())
                                    .collect(Collectors.toList()));
                    break;
                default:
                    logger.error("Unsupported related group property {}",
                            metadata.getRelatedGroupProperty());
            }
        });
    }
}
