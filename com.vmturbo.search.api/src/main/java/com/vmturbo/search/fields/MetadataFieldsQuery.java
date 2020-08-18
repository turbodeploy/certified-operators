package com.vmturbo.search.fields;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmturbo.api.dto.searchquery.EntityMetadataRequestApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO.Type;
import com.vmturbo.api.dto.searchquery.FieldValueTypeApiDTO;
import com.vmturbo.api.dto.searchquery.GroupMetadataRequestApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.enums.GroupType;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchGroupMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Handles request for getting supported metadataFields.
 */
public class MetadataFieldsQuery {

    /**
     * Supported {@link GroupType} metadata.
     */
    private static final List<GroupType> ALLOWABLE_GROUP_TYPES = Arrays.stream(SearchGroupMetadata.values())
            .map(SearchGroupMetadata::getGroupType)
            .collect(Collectors.toList());

    /**
     * Supported {@link EntityType} metadata.
     */
    private static final List<EntityType> ALLOWABLE_ENTITY_TYPES = Arrays.stream(SearchEntityMetadata.values())
            .map(SearchEntityMetadata::getEntityType)
            .collect(Collectors.toList());

    /**
     * Private constructor as expected of utility class.
     */
    private MetadataFieldsQuery(){}

    /**
     * Process request of {@link EntityMetadataRequestApiDTO}.
     * @param request the API entity field query input
     * @return list of available fields supporting request
     */
    public static List<FieldValueTypeApiDTO> processRequest(EntityMetadataRequestApiDTO request) {
        Map<FieldApiDTO, SearchMetadataMapping> metadataMappingMap =
                SearchEntityMetadata.valueOf(request.getEntityType().name()).getMetadataMappingMap();
        return translateMetadata(metadataMappingMap);
    }

    /**
     * Process request of {@link GroupMetadataRequestApiDTO}.
     * @param request the API entity field query input
     * @return list of available fields supporting request
     */
    public static List<FieldValueTypeApiDTO> processRequest(GroupMetadataRequestApiDTO request) {
        Map<FieldApiDTO, SearchMetadataMapping> metadataMappingMap =
                SearchGroupMetadata.valueOf(request.getGroupType().name()).getMetadataMappingMap();
        return translateMetadata(metadataMappingMap);
    }

    /**
     * Translates metadata to corresponding {@link FieldValueTypeApiDTO}.
     * @param metadataMappingMap Metadata values to translate
     * @return collection of {@link FieldValueTypeApiDTO}
     */
    private static List<FieldValueTypeApiDTO> translateMetadata(Map<FieldApiDTO, SearchMetadataMapping> metadataMappingMap) {
        Set<Entry<FieldApiDTO, SearchMetadataMapping>> entrySet = metadataMappingMap.entrySet();
        return entrySet.stream().map( entry -> {
                    FieldApiDTO fieldApiDTO = entry.getKey();
                    SearchMetadataMapping meta = entry.getValue();
                    Type type = meta.getApiDatatype();

                    if (fieldApiDTO.equals(PrimitiveFieldApiDTO.entityType())) {
                        return fieldApiDTO.ofValueType(ALLOWABLE_ENTITY_TYPES);
                    } else if (fieldApiDTO.equals(PrimitiveFieldApiDTO.groupType())) {
                        return fieldApiDTO.ofValueType(ALLOWABLE_GROUP_TYPES);
                    }

                    return type.equals(Type.ENUM)
                            ? fieldApiDTO.ofValueType((Class)meta.getEnumClass())
                            : fieldApiDTO.ofValueType(type);
                }).collect(Collectors.toList());
    }

}
