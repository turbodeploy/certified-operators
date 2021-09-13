package com.vmturbo.extractor.docgen.sections;

import static com.vmturbo.extractor.docgen.DocGenUtils.ALL;
import static com.vmturbo.extractor.docgen.DocGenUtils.DESCRIPTION;
import static com.vmturbo.extractor.docgen.DocGenUtils.JSON_TYPE;
import static com.vmturbo.extractor.docgen.DocGenUtils.REFERENCE;
import static com.vmturbo.extractor.docgen.DocGenUtils.REPEATED;
import static com.vmturbo.extractor.docgen.DocGenUtils.SHARED_DOC_PREFIX;
import static com.vmturbo.extractor.docgen.DocGenUtils.SUPPORTED_ENTITY_TYPES;
import static com.vmturbo.extractor.docgen.DocGenUtils.SUPPORTED_GROUP_TYPES;
import static com.vmturbo.extractor.docgen.DocGenUtils.TYPE;
import static com.vmturbo.extractor.export.ExportUtils.TAGS_JSON_KEY_NAME;
import static com.vmturbo.extractor.export.ExportUtils.TARGETS_JSON_KEY_NAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.Lists;

import com.vmturbo.api.dto.searchquery.FieldValueApiDTO.Type;
import com.vmturbo.extractor.docgen.DocGenUtils;
import com.vmturbo.extractor.docgen.Section;
import com.vmturbo.extractor.docgen.sections.BaseAttrsSection.AttrsItem;
import com.vmturbo.extractor.schema.json.export.Target;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Base Documentation section generator for Entity/Group Attributes. This section documents values
 * that may appear in the JSONB `entity.attrs` table column and also the attrs field in
 * {@link com.vmturbo.extractor.schema.json.export.Entity} or
 * {@link com.vmturbo.extractor.schema.json.export.Group}.
 *
 * @param <T> type of entity or group
 */
public abstract class BaseAttrsSection<T> extends Section<AttrsItem<T>> {

    /**
     * Create a new instance.
     *
     * @param sectionName section name
     * @param sectionPath full path of the section
     * @param docTree JSON structure with doc snippets to inject into the doc
     */
    public BaseAttrsSection(String sectionName, String sectionPath, JsonNode docTree) {
        super(sectionName, sectionPath, docTree);
    }

    @Override
    public List<AttrsItem<T>> getItems() {
        List<AttrsItem<T>> items = new ArrayList<>();
        // type specific info
        items.addAll(getJsonKeyToSupportedTypes().entrySet().stream()
                .map(entry -> {
                    SearchMetadataMapping metadata = entry.getKey();
                    final String type = apiTypeToDocType(metadata.getApiDatatype());
                    final String reference = metadata.getEnumClass() == null ? null
                            : DocGenUtils.ENUMS_DOC_PREFIX + "/" + metadata.getEnumClass().getSimpleName();
                    return new AttrsItem<>(entry.getValue(), metadata.getJsonKeyName(),
                            type, reference, metadata.getApiDatatype() == Type.MULTI_TEXT);
                }).collect(Collectors.toList()));

        // total hack to document tags value that can appear for any entity type, since it's
        // not really captured in metadata at due to limitations of type system in metadata
        // see PrimitiveFieldsOnTEDPatcher.extractAttrs
        items.add(new AttrsItem(Arrays.asList(getAllTypes()), TAGS_JSON_KEY_NAME,
                "Map (Embedded Reporting) or List (Data Exporter)", null, false));

        // targets
        items.add(new AttrsItem(Arrays.asList(getAllTypes()), TARGETS_JSON_KEY_NAME, JSON_TYPE,
                SHARED_DOC_PREFIX + "/" + Target.class.getSimpleName(), true));

        Collections.sort(items);
        return items;
    }

    @Override
    public List<String> getFieldNames(AttrsItem<T> item) {
        List<String> names = Lists.newArrayList(DESCRIPTION, TYPE);
        if (item.reference != null) {
            names.add(REFERENCE);
        }
        final String supportedTypeField = getSupportedTypeField();
        if (supportedTypeField != null && !supportedTypeField.isEmpty()) {
            names.add(supportedTypeField);
        }
        if (item.isList) {
            // this is a list of elements
            names.add(REPEATED);
        }
        return names;
    }

    @Override
    public JsonPointer getItemDocPath(final AttrsItem<T> item) {
        return docPathPrefix.append(JsonPointer.compile(String.format("/fields/%s", item.jsonKeyName)));
    }

    @Override
    public JsonNode getItemFieldValue(AttrsItem<T> item, String fieldName) {
        switch (fieldName) {
            case TYPE:
                return JsonNodeFactory.instance.textNode(item.type);
            case REFERENCE:
                return JsonNodeFactory.instance.textNode(item.reference);
            case SUPPORTED_ENTITY_TYPES:
            case SUPPORTED_GROUP_TYPES:
                if (new HashSet<>(item.supportedTypes).equals(new HashSet<>(
                        Arrays.asList(getAllTypes())))) {
                    return JsonNodeFactory.instance.textNode(ALL);
                } else {
                    return JsonNodeFactory.instance.textNode(item.supportedTypes.stream()
                            .map(this::typeToString)
                            .collect(Collectors.joining(",")));
                }
            case REPEATED:
                return JsonNodeFactory.instance.booleanNode(true);
        }
        return JsonNodeFactory.instance.nullNode();
    }

    /**
     * Convert api type to the type used in doc.
     *
     * @param apiType api type
     * @return type in doc
     */
    private String apiTypeToDocType(Type apiType) {
        switch (apiType) {
            case TEXT:
            case MULTI_TEXT:
            case ENUM:
                return String.class.getSimpleName();
            case BOOLEAN:
                return boolean.class.getSimpleName();
            case INTEGER:
                return Integer.class.getSimpleName();
            case NUMBER:
                return Double.class.getSimpleName();
        }
        return "";
    }

    /**
     * Get type specific info and its supported entity types.
     *
     * @return map
     */
    public abstract Map<SearchMetadataMapping, List<T>> getJsonKeyToSupportedTypes();

    /**
     * String representation of the type.
     *
     * @param type type of the entity
     * @return string
     */
    abstract String typeToString(T type);

    /**
     * Get all possible types of the entity.
     *
     * @return all types array
     */
    abstract T[] getAllTypes();

    /**
     * Get the field name for supported entity/group type.
     *
     * @return field name for supported type.
     */
    abstract String getSupportedTypeField();

    /**
     * Utility class used for items of the entity.attrs doc section.
     *
     * <p>With the exception of tags, which is not properly represented in metadata, the items
     * are derived by correlating metadata mappings with JSON keys, with {@link SearchEntityMetadata},
     * which is used to obtain meaningful names for the entries (falling back on JSON key).</p>
     *
     * <p>In the case of tags, we use a separate constructure that supplies all needed information
     * explicitly rather than attempting to pull them out of metadata.</p>
     *
     * @param <T> type of entity or group
     */
    static class AttrsItem<T> implements Comparable<AttrsItem<T>> {

        private final List<T> supportedTypes;
        private final String jsonKeyName;
        private final String type;
        private final String reference;
        private final boolean isList;

        /**
         * Create a new instance of EntityAttrsItem.
         *
         * @param supportedTypes list of supported entity/group types for this item
         * @param jsonKeyName name of json key for the item
         * @param type describing type of the item
         * @param reference describing reference of the item
         * @param isList if the field is a list
         */
        AttrsItem(List<T> supportedTypes, String jsonKeyName, String type, String reference, boolean isList) {
            this.supportedTypes = supportedTypes;
            this.jsonKeyName = jsonKeyName;
            this.type = type;
            this.reference = reference;
            this.isList = isList;
        }

        @Override
        public int compareTo(final AttrsItem other) {
            return jsonKeyName.compareTo(other.jsonKeyName);
        }
    }
}

