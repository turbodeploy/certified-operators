package com.vmturbo.extractor.docgen.sections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.extractor.docgen.Section;
import com.vmturbo.extractor.docgen.sections.EntityAttrsSection.EntityAttrsItem;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Documentation section generator for Entity Attributes. This section documents values that may
 * appear in the JSONB `entity.attrs` table column. Doc items will be grouped by entity type.
 */
public class EntityAttrsSection extends Section<EntityAttrsItem> {

    /**
     * Create a new instance.
     *
     * @param docTree JSON structure with doc snippets to inject into the doc
     */
    public EntityAttrsSection(JsonNode docTree) {
        super("entity.attrs", "/table_data/entity.attrs/", docTree);
    }

    @Override
    public List<EntityAttrsItem> getItems() {
        List<EntityAttrsItem> items = new ArrayList<>();
        PrimitiveFieldsOnTEDPatcher.getJsonbColumnMetadataByEntityType().forEach((key, value) -> {
            final EntityType entity_type = EntityType.forNumber(key);
            value.forEach(mapping ->
                    items.add(new EntityAttrsItem(entity_type, mapping)));
        });
        // total hack to document tags value that can appear for any entity type, since it's
        // not really captured in metadata at due to limitations of type system in metadata
        for (final EntityType entityType : EntityType.values()) {
            items.add(new EntityAttrsItem(entityType, "tags", "tags", "TEXT_MULTIMAP"));
        }
        Collections.sort(items);
        return items;
    }

    @Override
    public List<String> getFieldNames() {
        return ImmutableList.of("Entity_Type", "Attribute_Name", "Attribute_Type", "Description");
    }

    @Override
    public List<String> getGroupByFields() {
        return ImmutableList.of("Entity_Type");
    }

    @Override
    public JsonPointer getItemDocPath(final EntityAttrsItem item) {
        return docPathPrefix.append(JsonPointer.compile(String.format("/items/%s/%s",
                item.entityType, item.jsonKeyName)));
    }

    @Override
    public String getItemName(final EntityAttrsItem item) {
        return String.format("%s.%s", item.entityType, item.name);
    }

    @Override
    public String getType() {
        return "table_data";
    }

    @Override
    public Optional<String> getFieldValue(final EntityAttrsItem item, final String fieldName) {
        switch (fieldName) {
            case "Entity_Type":
                return Optional.of(item.entityType.name());
            case "Attribute_Name":
                return Optional.of(item.name);
            case "Attribute_Type":
                return Optional.of(item.type);
            default:
                return super.getFieldValue(item, fieldName);
        }
    }

    /**
     * Utility class used for items of the entity.attrs doc section.
     *
     * <p>With the exception of tags, which is not properly represented in metadata, the items
     * are derived by correlating metadata mappings with JSON keys, with {@link SearchEntityMetadata},
     * which is used to obtain meaningful names for the entries (falling back on JSON key).</p>
     *
     * <p>In the case of tags, we use a separate constructure that supplies all needed information
     * explicitly rather than attempting to pull them out of metadata.</p>
     */
    static class EntityAttrsItem implements Comparable<EntityAttrsItem> {

        private static final Table<EntityDTO.EntityType, SearchMetadataMapping, PrimitiveFieldApiDTO>
                mappingToApi = HashBasedTable.create();

        static {
            populateMappingToApiTable();
        }

        private final EntityType entityType;
        private final String name;
        private final String jsonKeyName;
        private final String type;

        /**
         * Create a new entry using metadata.
         *
         * @param entityType entity type for correlation of mapping to {@link SearchEntityMetadata}
         * @param mapping    {@link SearchMetadataMapping} instance
         */
        EntityAttrsItem(EntityDTO.EntityType entityType, SearchMetadataMapping mapping) {
            this.entityType = entityType;
            this.name = getApiName(entityType, mapping);
            this.jsonKeyName = mapping.getJsonKeyName() != null ? mapping.getJsonKeyName() : name;
            this.type = mapping.getApiDatatype().name();
        }

        /**
         * Create a new instance with all values explicitly provided, not derived from metadata..
         *
         * @param entityType  entity type
         * @param name        api name
         * @param jsonKeyName json key name
         * @param type        datatype
         */
        EntityAttrsItem(EntityDTO.EntityType entityType, String name, String jsonKeyName, String type) {
            this.entityType = entityType;
            this.name = name;
            this.jsonKeyName = jsonKeyName;
            this.type = type;
        }

        public static String getApiName(EntityType entityType, SearchMetadataMapping mapping) {
            final PrimitiveFieldApiDTO apiField = mappingToApi.get(entityType, mapping);
            return apiField != null ? apiField.getFieldName() : mapping.getJsonKeyName();
        }

        @Override
        public int compareTo(final EntityAttrsItem other) {
            int cmp = entityType.name().compareTo(other.entityType.name());
            return cmp != 0 ? cmp : jsonKeyName.compareTo(other.jsonKeyName);
        }

        private static void populateMappingToApiTable() {
            for (final SearchEntityMetadata value : SearchEntityMetadata.values()) {
                EntityDTO.EntityType entityType = EntityDTO.EntityType.forNumber(
                        ApiEntityType.fromStringToSdkType(value.getEntityType().name()));
                value.getMetadataMappingMap().entrySet().stream()
                        .filter(e -> e.getKey() instanceof PrimitiveFieldApiDTO)
                        .forEach(e -> mappingToApi.put(entityType, e.getValue(),
                                (PrimitiveFieldApiDTO)e.getKey()));
            }
        }
    }
}

