package com.vmturbo.extractor.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchGroupMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Utils for obtaining metadata for entity and group during search data ingestion.
 */
public class SearchMetadataUtils {

    /**
     * Private constructor.
     */
    private SearchMetadataUtils() {}

    /**
     * List of search entity metadata by entity type and field type. We use integer entity type
     * as key to match the int entity type in
     * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO}, so we don't
     * need to convert the int entity type back to api {@link EntityType} for every
     * TopologyEntityDTO during ingestion.
     */
    private static final Table<Integer, FieldType, List<SearchMetadataMapping>>
            METADATA_BY_ENTITY_TYPE_AND_FIELD_TYPE;

    static {
        final Table<Integer, FieldType, List<SearchMetadataMapping>> table = HashBasedTable.create();
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
            int entityType = EnumUtils.entityTypeFromApiToProto(
                    searchEntityMetadata.getEntityType()).getNumber();
            searchEntityMetadata.getMetadataMappingMap().forEach((fieldApiDTO, metadata) -> {
                List<SearchMetadataMapping> metadataList =
                        table.get(entityType, fieldApiDTO.getFieldType());
                if (metadataList == null) {
                    metadataList = new ArrayList<>();
                    table.put(entityType, fieldApiDTO.getFieldType(), metadataList);
                }
                metadataList.add(metadata);
            });
        }
        METADATA_BY_ENTITY_TYPE_AND_FIELD_TYPE = Tables.unmodifiableTable(table);
    }

    /**
     * list of search entity metadata by group type and field type.
     */
    private static final Table<GroupType, FieldType, List<SearchMetadataMapping>>
            METADATA_BY_GROUP_TYPE_AND_FIELD_TYPE;

    static {
        final Table<GroupType, FieldType, List<SearchMetadataMapping>> table = HashBasedTable.create();
        for (SearchGroupMetadata searchGroupMetadata : SearchGroupMetadata.values()) {
            GroupType groupType = EnumUtils.groupTypeFromApiToProto(
                searchGroupMetadata.getGroupType());

            searchGroupMetadata.getMetadataMappingMap().forEach((fieldApiDTO, metadata) -> {
                List<SearchMetadataMapping> metadataList =
                        table.get(groupType, fieldApiDTO.getFieldType());
                if (metadataList == null) {
                    metadataList = new ArrayList<>();
                    table.put(groupType, fieldApiDTO.getFieldType(), metadataList);
                }
                metadataList.add(metadata);
            });
        }
        METADATA_BY_GROUP_TYPE_AND_FIELD_TYPE = Tables.unmodifiableTable(table);
    }

    /**
     * Set of commodity types for different entity types whose values (used, capacity, percentile)
     * will be cached for use by groups and related entities later.
     */
    private static final Map<Integer, Set<Integer>> COMMODITY_TYPES_TO_SCRAPE_BY_ENTITY_TYPE;

    static {
        // collect all commodity types for different entity types we care which we would
        // like to cache for use later by groups/related entities
        Map<Integer, Set<Integer>> commodityTypesByEntityType = new HashMap<>();
        // try to collect from group metadata
        for (SearchGroupMetadata searchGroupMetadata : SearchGroupMetadata.values()) {
            searchGroupMetadata.getMetadataMappingMap().forEach((fieldApiDTO, metadata) -> {
                if (fieldApiDTO.getFieldType() == FieldType.AGGREGATE_COMMODITY) {
                    int entityType = EnumUtils.entityTypeFromApiToProto(
                            metadata.getMemberType()).getNumber();
                    commodityTypesByEntityType.computeIfAbsent(entityType, k -> new HashSet<>())
                            .add(EnumUtils.commodityTypeFromApiToProtoInt(metadata.getCommodityType()));
                }
            });
        }
        // try to collect from entity metadata, we only need to cache those related entities'
        // commodities, since direct commodity on the entity can be obtained from TopologyEntityDTO directly
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
            searchEntityMetadata.getMetadataMappingMap().forEach((fieldApiDTO, metadata) -> {
                if (fieldApiDTO.getFieldType() == FieldType.COMMODITY
                        && metadata.getRelatedEntityTypes() != null) {
                    metadata.getRelatedEntityTypes().forEach(apiEntityType -> {
                        int entityType = EnumUtils.entityTypeFromApiToProto(
                                apiEntityType).getNumber();
                        commodityTypesByEntityType.computeIfAbsent(entityType, k -> new HashSet<>())
                                .add(EnumUtils.commodityTypeFromApiToProtoInt(metadata.getCommodityType()));
                    });
                }
            });
        }
        COMMODITY_TYPES_TO_SCRAPE_BY_ENTITY_TYPE = Collections.unmodifiableMap(commodityTypesByEntityType);
    }

    /**
     * Get list of defined metadata for the given entity type and field type.
     *
     * @param entityType type of the entity
     * @param fieldType type of the field as defined in {@link FieldType}
     * @return list of {@link SearchMetadataMapping}
     */
    @Nonnull
    public static List<SearchMetadataMapping> getMetadata(int entityType, FieldType fieldType) {
        List<SearchMetadataMapping> metadataMappingList =
                METADATA_BY_ENTITY_TYPE_AND_FIELD_TYPE.get(entityType, fieldType);
        return metadataMappingList != null ? metadataMappingList : Collections.emptyList();
    }

    /**
     * Check if there is metadata defined for the given entity type.
     *
     * @param entityType type of the entity
     * @return true if metadata is defined for the entity, otherwise false
     */
    public static boolean hasMetadata(int entityType) {
        Map<FieldType, List<SearchMetadataMapping>> metadata =
                METADATA_BY_ENTITY_TYPE_AND_FIELD_TYPE.row(entityType);
        return metadata != null && !metadata.isEmpty();
    }

    /**
     * Get list of defined metadata for the given group type and field type.
     *
     * @param groupType type of the group
     * @param fieldType type of the field as defined in {@link FieldType}
     * @return list of {@link SearchMetadataMapping}
     */
    @Nonnull
    public static List<SearchMetadataMapping> getMetadata(GroupType groupType, FieldType fieldType) {
        List<SearchMetadataMapping> metadataMappingList =
                METADATA_BY_GROUP_TYPE_AND_FIELD_TYPE.get(groupType, fieldType);
        return metadataMappingList != null ? metadataMappingList : Collections.emptyList();
    }

    /**
     * Check if there is metadata defined for the given group type.
     *
     * @param groupType type of the group
     * @return true if metadata is defined for the group, otherwise false
     */
    public static boolean hasMetadata(GroupType groupType) {
        Map<FieldType, List<SearchMetadataMapping>> metadata =
                METADATA_BY_GROUP_TYPE_AND_FIELD_TYPE.row(groupType);
        return metadata != null && !metadata.isEmpty();
    }

    /**
     * Get the set of commodity types which should be cached for use later.
     *
     * @param entityType type of the entity
     * @return set of commodity types
     */
    @Nonnull
    public static Set<Integer> getCommodityTypesToScrape(int entityType) {
        return COMMODITY_TYPES_TO_SCRAPE_BY_ENTITY_TYPE.getOrDefault(entityType,
                Collections.emptySet());
    }
}
