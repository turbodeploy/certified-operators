package com.vmturbo.search.metadata;

import static com.vmturbo.search.metadata.SearchGroupMetadata.Constants.GROUP_COMMON_FIELDS;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.dto.searchquery.AggregateCommodityFieldApiDTO;
import com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.MemberFieldApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedActionFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedEntityFieldApiDTO;
import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.enums.GroupType;

/**
 * Enumeration of search db column mappings for group, which is used by both ingestion and query.
 */
public enum SearchGroupMetadata {

    /**
     * Mappings for different group types.
     */
    REGULAR(GroupType.REGULAR, getRegularMetadata()),
    COMPUTE_HOST_CLUSTER(GroupType.COMPUTE_HOST_CLUSTER, getComputeHostClusterMetadata()),
    STORAGE_CLUSTER(GroupType.STORAGE_CLUSTER, GROUP_COMMON_FIELDS),
    COMPUTE_VIRTUAL_MACHINE_CLUSTER(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER, GROUP_COMMON_FIELDS),
    RESOURCE(GroupType.RESOURCE, GROUP_COMMON_FIELDS),
    BILLING_FAMILY(GroupType.BILLING_FAMILY, GROUP_COMMON_FIELDS);

    private final GroupType groupType;

    private final Map<FieldApiDTO, SearchMetadataMapping> metadataMappingMap;

    /**
     * Create a SearchEntityMetadata, containing column mappings for reading/writing to searchDB.
     *
     * @param groupType GroupType which mappings belong to
     * @param metadataMappingMap mappings for the GroupType
     */
    SearchGroupMetadata(@Nonnull GroupType groupType,
            @Nonnull Map<FieldApiDTO, SearchMetadataMapping> metadataMappingMap) {
        this.groupType = groupType;
        this.metadataMappingMap = metadataMappingMap;
    }

    public GroupType getGroupType() {
        return groupType;
    }

    /**
     * Gets an immutable map representing the mappings for fields specific to this group type.
     *
     * @return an immutable map representing the mappings for fields specific to this group type
     */
    public Map<FieldApiDTO, SearchMetadataMapping> getMetadataMappingMap() {
        return metadataMappingMap;
    }

    /**
     * Gets an immutable map representing the mappings for fields common to all groups.
     *
     * @return an immutable map representing the mappings for fields common to all groups
     */
    public static Map<FieldApiDTO, SearchMetadataMapping> getGroupCommonFieldsMappingMap() {
        return GROUP_COMMON_FIELDS;
    }

    private static Map<FieldApiDTO, SearchMetadataMapping> getRegularMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(GROUP_COMMON_FIELDS)
                .build();
    }

    private static Map<FieldApiDTO, SearchMetadataMapping> getComputeHostClusterMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(GROUP_COMMON_FIELDS)
                // member counts
                .put(MemberFieldApiDTO.memberCount(EntityType.PHYSICAL_MACHINE),
                        SearchMetadataMapping.DIRECT_MEMBER_COUNT_PM)
                .put(RelatedEntityFieldApiDTO.entityCount(EntityType.VIRTUAL_MACHINE),
                        SearchMetadataMapping.RELATED_MEMBER_COUNT_VM)
                .put(RelatedEntityFieldApiDTO.entityCount(EntityType.STORAGE),
                        SearchMetadataMapping.RELATED_MEMBER_COUNT_ST)
                // aggregated commodities
                .put(AggregateCommodityFieldApiDTO.total(CommodityFieldApiDTO.utilization(CommodityType.CPU)),
                        SearchMetadataMapping.GROUP_COMMODITY_CPU_UTILIZATION_TOTAL)
                .put(AggregateCommodityFieldApiDTO.total(CommodityFieldApiDTO.utilization(CommodityType.MEM)),
                        SearchMetadataMapping.GROUP_COMMODITY_MEM_UTILIZATION_TOTAL)
                .build();
    }

    /**
     * Put static fields inside a nested class rather than inside the enum class, since enum
     * constructor is called BEFORE the static fields have all been initialized.
     */
    static class Constants {
        /**
         * Common fields available to all groups.
         */
        static final Map<FieldApiDTO, SearchMetadataMapping> GROUP_COMMON_FIELDS = ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // PRIMITIVES
                .put(PrimitiveFieldApiDTO.oid(), SearchMetadataMapping.PRIMITIVE_GROUP_OID)
                .put(PrimitiveFieldApiDTO.groupType(), SearchMetadataMapping.PRIMITIVE_GROUP_TYPE)
                .put(PrimitiveFieldApiDTO.name(), SearchMetadataMapping.PRIMITIVE_GROUP_NAME)
                .put(PrimitiveFieldApiDTO.severity(), SearchMetadataMapping.PRIMITIVE_SEVERITY)
                .put(PrimitiveFieldApiDTO.origin(), SearchMetadataMapping.PRIMITIVE_GROUP_ORIGIN)
                .put(PrimitiveFieldApiDTO.dynamic(), SearchMetadataMapping.PRIMITIVE_GROUP_DYNAMIC)
                .put(PrimitiveFieldApiDTO.memberTypes(), SearchMetadataMapping.PRIMITIVE_GROUP_MEMBER_TYPES)
                // MEMBERS
                .put(MemberFieldApiDTO.memberCount(), SearchMetadataMapping.DIRECT_MEMBER_COUNT)
                // todo: uncomment if indirect member types is needed
                // .put(PrimitiveFieldApiDTO.indirectMemberTypes(), SearchMetadataMapping.PRIMITIVE_GROUP_INDIRECT_MEMBER_TYPES)
                // RELATED ACTION
                .put(RelatedActionFieldApiDTO.actionCount(), SearchMetadataMapping.RELATED_ACTION_COUNT)
                .build();
    }
}
