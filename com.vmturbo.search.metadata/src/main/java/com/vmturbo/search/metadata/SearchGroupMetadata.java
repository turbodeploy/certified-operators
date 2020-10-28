package com.vmturbo.search.metadata;

import static com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO.currentUtilization;
import static com.vmturbo.api.dto.searchquery.RelatedEntityFieldApiDTO.entityNames;
import static com.vmturbo.search.metadata.SearchGroupMetadata.Constants.COMPUTE_HOST_CLUSTER_METADATA;
import static com.vmturbo.search.metadata.SearchGroupMetadata.Constants.GROUP_COMMON_FIELDS;
import static com.vmturbo.search.metadata.SearchGroupMetadata.Constants.RESOURCE_GROUP_METADATA;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_BUSINESS_ACCOUNT;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.dto.searchquery.AggregateCommodityFieldApiDTO;
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
    BillingFamily(GroupType.BillingFamily, GROUP_COMMON_FIELDS),
    Cluster(GroupType.Cluster, COMPUTE_HOST_CLUSTER_METADATA),
    VMCluster(GroupType.VMCluster, GROUP_COMMON_FIELDS),
    Group(GroupType.Group, GROUP_COMMON_FIELDS),
    Resource(GroupType.Resource, RESOURCE_GROUP_METADATA),
    StorageCluster(GroupType.StorageCluster, GROUP_COMMON_FIELDS);

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

        static final Map<FieldApiDTO, SearchMetadataMapping> RESOURCE_GROUP_METADATA = ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(GROUP_COMMON_FIELDS)
                // related entities
                //TODO: resource group and business account relation should be handled separately as complex data
                .put(entityNames(EntityType.BusinessAccount), RELATED_BUSINESS_ACCOUNT)
                .build();

        static final Map<FieldApiDTO, SearchMetadataMapping> COMPUTE_HOST_CLUSTER_METADATA = ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(GROUP_COMMON_FIELDS)
                // member counts
                .put(MemberFieldApiDTO.memberCount(EntityType.PhysicalMachine),
                        SearchMetadataMapping.DIRECT_MEMBER_COUNT_PM)
                .put(RelatedEntityFieldApiDTO.entityCount(EntityType.VirtualMachine),
                        SearchMetadataMapping.RELATED_MEMBER_COUNT_VM)
                .put(RelatedEntityFieldApiDTO.entityCount(EntityType.Storage),
                        SearchMetadataMapping.RELATED_MEMBER_COUNT_ST)
                // aggregated commodities
                // TODO: (OM-60754) Change this to be average over last 24 hours, when that data is available
                .put(AggregateCommodityFieldApiDTO.total(currentUtilization(CommodityType.CPU)),
                        SearchMetadataMapping.GROUP_COMMODITY_CPU_HISTORICAL_UTILIZATION_TOTAL)
                .put(AggregateCommodityFieldApiDTO.total(currentUtilization(CommodityType.MEM)),
                        SearchMetadataMapping.GROUP_COMMODITY_MEM_HISTORICAL_UTILIZATION_TOTAL)
                .build();
    }
}
