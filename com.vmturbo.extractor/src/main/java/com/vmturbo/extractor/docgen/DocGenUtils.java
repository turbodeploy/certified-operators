package com.vmturbo.extractor.docgen;

import static com.vmturbo.extractor.schema.tables.SearchEntity.SEARCH_ENTITY;
import static com.vmturbo.extractor.schema.tables.SearchEntityAction.SEARCH_ENTITY_ACTION;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.google.common.collect.ImmutableSet;

import org.jooq.Table;

import com.vmturbo.extractor.docgen.sections.JsonSection;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.schema.json.export.ExporterField;

/**
 * Utils for doc generation.
 */
public class DocGenUtils {

    /**
     * Private constructor to prevent creating new instance.
     */
    private DocGenUtils() {
    }

    /**
     * Doc path prefix for data exporter section.
     */
    public static final String DATA_EXPORTER_DOC_PREFIX = "/exporter";

    /**
     * Doc path prefix for the tables section in embedded reporting.
     */
    public static final String EMBEDDED_REPORTING_TABLE_DOC_PREFIX = "/tables";

    /**
     * Doc path prefix for the enums section in embedded reporting.
     */
    public static final String ENUMS_DOC_PREFIX = "/enums";

    /**
     * Doc path prefix for the shared section between data exporter and embedded reporting.
     */
    public static final String SHARED_DOC_PREFIX = "/shared";

    /**
     * Doc path prefix for the table data (usually a jsonb column) in embedded reporting.
     */
    public static final String TABLE_DATA_DOC_PREFIX = "/table_data";

    /**
     * Section name for the action attrs column in embedded reporting.
     */
    public static final String ACTION_ATTRS_SECTION_NAME = "action.attrs";

    /**
     * Section name for the entity attrs column in embedded reporting and data exporter.
     */
    public static final String ENTITY_ATTRS_SECTION_NAME = "entity.attrs";

    /**
     * Section name for the group attrs column in embedded reporting and data exporter.
     */
    public static final String GROUP_ATTRS_SECTION_NAME = "group.attrs";

    /**
     * Doc path for entity attrs.
     */
    public static final String ENTITY_ATTRS_SECTION_PATH = SHARED_DOC_PREFIX + "/" + ENTITY_ATTRS_SECTION_NAME;

    /**
     * Doc path for group attrs.
     */
    public static final String GROUP_ATTRS_SECTION_PATH = SHARED_DOC_PREFIX + "/" + GROUP_ATTRS_SECTION_NAME;

    /**
     * Description of the field.
     */
    public static final String DESCRIPTION = "Description";

    /**
     * Type of the field.
     */
    public static final String TYPE = "Type";

    /**
     * Whether the field value is a list.
     */
    public static final String REPEATED = "Repeated";

    /**
     * Format of the string field.
     */
    public static final String FORMAT = "Format";

    /**
     * Whether the column is nullable.
     */
    public static final String NULLABLE = "Nullable";

    /**
     * Whether the column is primary.
     */
    public static final String PRIMARY = "Primary";

    /**
     * Reference type of the column.
     */
    public static final String REFERENCE = "Reference";

    /**
     * Enum type.
     */
    public static final String ENUM_TYPE = "enum";

    /**
     * Type of the map key if this field is a map.
     */
    public static final String MAP_KEY_TYPE = "MapKeyType";

    /**
     * Supported entity types of this item.
     */
    public static final String SUPPORTED_ENTITY_TYPES = "SupportedEntityTypes";

    /**
     * Supported group types of this item.
     */
    public static final String SUPPORTED_GROUP_TYPES = "SupportedGroupTypes";

    /**
     * Type indicating the field is a json.
     */
    public static final String JSON_TYPE = "JSON";

    /**
     * Notes for the field.
     */
    public static final String NOTES_FIELD = "Notes";

    /**
     * Whether this item is available to all entity or group types.
     */
    public static final String ALL = "ALL";

    /**
     * Tables not for reporting purposes.
     */
    public static final Set<Table<?>> NON_REPORTING_TABLES = ImmutableSet.of(
            SEARCH_ENTITY, SEARCH_ENTITY_ACTION
    );

    private static final ObjectMapper mapper = ExportUtils.objectMapper;

    /**
     * Get all the documentation sections within given class. We perform a BFS starting from the
     * given class (using jackson library to introspect the fields which will be dumped to json)
     * and traverse the nested fields until the leaf.
     *
     * @param startClass the starting POJO
     * @param startFieldName name for the starting class
     * @param docPathPrefix prefix path for the doc
     * @param docTree the doc tree to build
     * @param sharedClasses used to track shared classes
     * @return list of sections
     */
    public static List<Section<?>> getSections(Class<?> startClass, String startFieldName,
            String docPathPrefix, JsonNode docTree, Set<Class<?>> sharedClasses) {
        List<Section<?>> jsonSections = new ArrayList<>();

        Deque<Node> queue = new ArrayDeque<>();
        // Construct a Jackson JavaType for the starting class
        JavaType startJavaType = mapper.getTypeFactory().constructType(startClass);
        queue.offer(new Node(startJavaType, startFieldName, false));

        while (!queue.isEmpty()) {
            int size = queue.size();

            for (int i = 0; i < size; i++) {
                Node node = queue.poll();
                String sectionName = node.javaType.getRawClass() == startClass
                        ? startFieldName : node.javaType.getRawClass().getSimpleName();
                ExporterField classAnnotation = node.javaType.getRawClass().getAnnotation(ExporterField.class);
                // if the field is shared or the whole class is shared, then put under shared section
                String prefix = (node.shared || (classAnnotation != null && classAnnotation.shared()))
                        ? DocGenUtils.SHARED_DOC_PREFIX
                        : docPathPrefix;

                List<BeanPropertyDefinition> properties = getProperties(node.javaType);
                if (properties.isEmpty()) {
                    // do not traverse further from primitive fields like entity.oid
                    continue;
                }

                // new section
                jsonSections.add(new JsonSection(docTree, node.javaType, sectionName, prefix));

                properties.forEach(property -> {
                    JavaType type = property.getPrimaryType();
                    String propertyName = property.getName();
                    ExporterField annotation = property.hasField()
                            ? property.getField().getAnnotation(ExporterField.class) : null;

                    // collect shared classes and do not traverse further
                    if (sharedClasses != null && annotation != null && annotation.shared()) {
                        if (type.isMapLikeType() || type.isCollectionLikeType()) {
                            sharedClasses.add(type.getContentType().getRawClass());
                        } else {
                            sharedClasses.add(type.getRawClass());
                        }
                        return;
                    }

                    if (type.isMapLikeType()) {
                        if (annotation != null && annotation.mapKeyEnum().length > 0) {
                            JavaType contentType = type.getContentType();
                            if (contentType.isCollectionLikeType()) {
                                contentType = contentType.getContentType();
                            }
                            queue.offer(new Node(contentType, propertyName, annotation.shared()));
                        } else {
                            System.out.println("Map property without annotation: " + property);
                        }
                    } else if (type.isCollectionLikeType()) {
                        // like: List<ActionEntity>, use the ActionEntity inside List as section name
                        queue.offer(new Node(type.getContentType(),
                                type.getContentType().getRawClass().getSimpleName(),
                                Optional.ofNullable(annotation).map(ExporterField::shared).orElse(false)));
                    } else {
                        queue.offer(new Node(type, propertyName,
                                Optional.ofNullable(annotation).map(ExporterField::shared).orElse(false)));
                    }
                });
            }
        }

        return jsonSections;
    }

    /**
     * Get all the properties inside given java type.
     *
     * @param javaType the jackson wrapper around the raw class
     * @return list of properties which exist in json
     */
    public static List<BeanPropertyDefinition> getProperties(JavaType javaType) {
        if (DocGenUtils.hasNoNestedFields(javaType)) {
            // no need to introspect for classes wrapping primitive types
            return Collections.emptyList();
        }

        // if it's a list of objects, then introspect the object in the list
        if (javaType.isCollectionLikeType()) {
            javaType = javaType.getContentType();
        }

        // Introspect the given type
        BeanDescription beanDescription = mapper.getSerializationConfig().introspect(javaType);
        // Find properties
        List<BeanPropertyDefinition> properties = beanDescription.findProperties();
        // Get class level ignored properties
        Set<String> ignoredProperties = mapper.getSerializationConfig().getAnnotationIntrospector()
                .findPropertyIgnorals(beanDescription.getClassInfo()).getIgnored();
        // Filter properties removing the class level ignored ones
        return properties.stream()
                .filter(property -> !ignoredProperties.contains(property.getName()))
                .collect(Collectors.toList());
    }

    /**
     * Check whether there are nested fields in the given type.
     *
     * @param javaType type of a json field
     * @return true if no nested fields, otherwise false
     */
    public static boolean hasNoNestedFields(JavaType javaType) {
        return javaType.isPrimitive()
                || javaType.isTypeOrSubTypeOf(Number.class)
                || javaType.isTypeOrSubTypeOf(String.class)
                || javaType.isTypeOrSubTypeOf(Boolean.class)
                || javaType.isTypeOrSubTypeOf(Character.class)
                || javaType.isTypeOrSubTypeOf(Byte.class);
    }

    /**
     * Helper class for traversing all the nested fields inside a class.
     */
    private static class Node {
        JavaType javaType;
        String name;
        boolean shared;

        Node(JavaType javaType, String name, boolean shared) {
            this.javaType = javaType;
            this.name = name;
            this.shared = shared;
        }
    }
}
