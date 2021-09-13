package com.vmturbo.extractor.docgen.sections;

import static com.vmturbo.extractor.docgen.DocGenUtils.DESCRIPTION;
import static com.vmturbo.extractor.docgen.DocGenUtils.ENTITY_ATTRS_SECTION_PATH;
import static com.vmturbo.extractor.docgen.DocGenUtils.ENUM_TYPE;
import static com.vmturbo.extractor.docgen.DocGenUtils.FORMAT;
import static com.vmturbo.extractor.docgen.DocGenUtils.GROUP_ATTRS_SECTION_PATH;
import static com.vmturbo.extractor.docgen.DocGenUtils.JSON_TYPE;
import static com.vmturbo.extractor.docgen.DocGenUtils.MAP_KEY_TYPE;
import static com.vmturbo.extractor.docgen.DocGenUtils.REFERENCE;
import static com.vmturbo.extractor.docgen.DocGenUtils.REPEATED;
import static com.vmturbo.extractor.docgen.DocGenUtils.TYPE;

import java.util.List;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.Lists;

import com.vmturbo.extractor.docgen.DocGenUtils;
import com.vmturbo.extractor.docgen.Section;
import com.vmturbo.extractor.schema.json.export.Entity;
import com.vmturbo.extractor.schema.json.export.ExporterField;
import com.vmturbo.extractor.schema.json.export.Group;

/**
 * Documentation section generator for a json object.
 */
public class JsonSection extends Section<BeanPropertyDefinition> {

    private final JavaType javaType;
    private final String docPathPrefixStr;

    /**
     * Create a new instance.
     *
     * @param docTree JSON structure containing document snippets
     * @param javaType JavaType of the class representing the section
     * @param sectionName name of this section
     * @param docPathPrefix prefix of the doc path for this section
     */
    public JsonSection(final JsonNode docTree, JavaType javaType, String sectionName, String docPathPrefix) {
        super(sectionName, docPathPrefix + "/" + sectionName, docTree);
        this.docPathPrefixStr = docPathPrefix;
        this.javaType = javaType;
    }

    @Override
    public List<BeanPropertyDefinition> getItems() {
        return DocGenUtils.getProperties(javaType);
    }

    @Override
    public List<String> getFieldNames(BeanPropertyDefinition item) {
        List<String> docFields = Lists.newArrayList(DESCRIPTION, TYPE);
        if (!getReference(item).isNull()) {
            docFields.add(REFERENCE);
        }
        ExporterField annotation = item.getField()  == null
                ? null : item.getField().getAnnotation(ExporterField.class);
        if (item.getPrimaryType().isMapLikeType()) {
            // Map<EntityType, List<RelatedEntity>>
            if (annotation == null || !annotation.basedOnMetadata()) {
                docFields.add(MAP_KEY_TYPE);
            }
            if (item.getPrimaryType().getContentType().isCollectionLikeType()) {
                // List<RelatedEntity>
                docFields.add(REPEATED);
            }
        } else if (item.getPrimaryType().isCollectionLikeType()) {
            // List<ActionEntity>
            docFields.add(REPEATED);
        }

        if (item.hasField() && annotation != null && !annotation.format().isEmpty()) {
            docFields.add(FORMAT);
        }
        return docFields;
    }

    @Override
    public JsonNode getItemFieldValue(BeanPropertyDefinition item, String fieldName) {
        switch (fieldName) {
            case TYPE:
                return getType(item);
            case REFERENCE:
                return getReference(item);
            case MAP_KEY_TYPE:
                final ExporterField annotation = item.hasField() ? item.getField().getAnnotation(ExporterField.class) : null;
                // Map<EntityType, MoveInfo>
                if (annotation != null) {
                    return JsonNodeFactory.instance.textNode(DocGenUtils.ENUMS_DOC_PREFIX + "/"
                            + annotation.mapKeyEnum()[0].getEnumConstants()[0].getName());
                } else {
                    return JsonNodeFactory.instance.textNode(item.getPrimaryType().getKeyType().getRawClass().getSimpleName());
                }
            case REPEATED:
                return JsonNodeFactory.instance.booleanNode(true);
            case FORMAT:
                return JsonNodeFactory.instance.textNode(item.getField().getAnnotation(ExporterField.class).format());
        }
        return null;
    }

    @Override
    public JsonPointer getItemDocPath(final BeanPropertyDefinition field) {
        return docPathPrefix.append(JsonPointer.compile("/fields/" + field.getName()));
    }

    /**
     * Get type of the given item, like is it a enum, json or String.
     *
     * @param item property in a json
     * @return type of item
     */
    private JsonNode getType(BeanPropertyDefinition item) {
        final JavaType primaryType = item.getPrimaryType();
        if (primaryType.isMapLikeType() || primaryType.isCollectionLikeType()) {
            JavaType contentType = primaryType.getContentType();
            if (contentType.isCollectionLikeType()) {
                contentType = contentType.getContentType();
            }
            if (DocGenUtils.hasNoNestedFields(contentType)) {
                return JsonNodeFactory.instance.textNode(contentType.getRawClass().getSimpleName());
            }
        } else if (DocGenUtils.hasNoNestedFields(primaryType)) {
            return JsonNodeFactory.instance.textNode(primaryType.getRawClass().getSimpleName());
        } else if (isEnum(item)) {
            return JsonNodeFactory.instance.textNode(ENUM_TYPE);
        }
        return JsonNodeFactory.instance.textNode(JSON_TYPE);
    }

    /**
     * Get reference of the given item, like /shared/MoveChange.
     *
     * @param item property in a json
     * @return reference of item
     */
    private JsonNode getReference(BeanPropertyDefinition item) {
        final JavaType primaryType = item.getPrimaryType();
        final ExporterField annotation = item.hasField() ? item.getField().getAnnotation(ExporterField.class) : null;
        if (isEnum(item)) {
            // value of this field is an enum, like entity.type
            return JsonNodeFactory.instance.textNode(DocGenUtils.ENUMS_DOC_PREFIX + "/"
                    + annotation.valueEnum()[0].getEnumConstants()[0].getName());
        } else if (primaryType.isMapLikeType()) {
            if (item.getName().equals("attrs") && item.getField().getDeclaringClass() == Entity.class) {
                // special case for entity.attrs
                return JsonNodeFactory.instance.textNode(ENTITY_ATTRS_SECTION_PATH);
            } else if (item.getName().equals("attrs") && item.getField().getDeclaringClass() == Group.class) {
                // special case for group.attrs
                return JsonNodeFactory.instance.textNode(GROUP_ATTRS_SECTION_PATH);
            } else {
                // if shared, put under common section
                final String prefix = annotation != null && annotation.shared()
                        ? DocGenUtils.SHARED_DOC_PREFIX : this.docPathPrefixStr;
                // this field is a map, then its type should be the type of the value object
                JavaType contentType = primaryType.getContentType();
                if (contentType.isCollectionLikeType()) {
                    // if the value is a list, then use the element inside the list
                    contentType = contentType.getContentType();
                }
                if (!DocGenUtils.hasNoNestedFields(contentType)) {
                    return JsonNodeFactory.instance.textNode(prefix + "/" + contentType.getRawClass().getSimpleName());
                }
            }
        } else if (primaryType.isCollectionLikeType()) {
            JavaType contentType = primaryType.getContentType();
            if (!DocGenUtils.hasNoNestedFields(contentType)) {
                return JsonNodeFactory.instance.textNode(this.docPathPrefixStr + "/"
                        + primaryType.getContentType().getRawClass().getSimpleName());
            }
        } else if (!DocGenUtils.hasNoNestedFields(primaryType)) {
            // has nested fields
            // like: type for action.deleteInfo is DeleteInfo
            final String prefix = annotation != null && annotation.shared()
                    ? DocGenUtils.SHARED_DOC_PREFIX : this.docPathPrefixStr;
            return JsonNodeFactory.instance.textNode(prefix + "/"
                    + primaryType.getRawClass().getSimpleName());
        }
        // no reference
        return JsonNodeFactory.instance.nullNode();
    }

    /**
     * Check if the item is referring to an enum.
     *
     * @param item property in a json
     * @return true if item is referring to an enum, or false if not
     */
    private boolean isEnum(BeanPropertyDefinition item) {
        final ExporterField annotation = item.hasField() ? item.getField().getAnnotation(ExporterField.class) : null;
        return annotation != null && annotation.valueEnum().length > 0;
    }
}
