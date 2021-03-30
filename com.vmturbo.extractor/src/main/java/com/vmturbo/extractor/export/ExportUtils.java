package com.vmturbo.extractor.export;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.schema.json.export.ExportedObject;
import com.vmturbo.extractor.search.EnumUtils.CommodityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.EntityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.GroupTypeUtils;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Utils for data extraction.
 */
public class ExportUtils {

    /**
     * The final topic with namespace will be turbonomic.exporter.
     */
    public static final String DATA_EXTRACTION_KAFKA_TOPIC = "exporter";

    /**
     * The date format for time field in the exported object.
     */
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        // serialize all fields even through no getter defined or private
        objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
    }

    /**
     * JSON key for tags values in attrs.
     */
    public static final String TAGS_JSON_KEY_NAME = "tags";

    /**
     * Private constructor to avoid creating new instance.
     */
    private ExportUtils() {}

    /**
     * Get the formatted date for given timestamp.
     *
     * @param timestamp time in millis
     * @return formatted date
     */
    public static String getFormattedDate(long timestamp) {
        return dateFormat.format(timestamp);
    }

    /**
     * Get the string of the entity type which is used as a key in json object, like VIRTUAL_MACHINE.
     *
     * @param protoEntityTypeInt entity type in proto integer
     * @return entity type string used in json object
     */
    @Nullable
    public static String getEntityTypeJsonKey(int protoEntityTypeInt) {
        com.vmturbo.extractor.schema.enums.EntityType entityType =
                EntityTypeUtils.protoIntToDb(protoEntityTypeInt, null);
        return entityType == null ? null : entityType.getLiteral();
    }

    /**
     * Get the string of the group type which is used as a key in json object, like COMPUTE_CLUSTER.
     *
     * @param groupType group type
     * @return group type string used in json object
     */
    @Nullable
    public static String getGroupTypeJsonKey(GroupType groupType) {
        com.vmturbo.extractor.schema.enums.EntityType dbType =
                GroupTypeUtils.protoToDb(groupType);
        return dbType == null ? null : dbType.name();
    }

    /**
     * Get the string of the commodity type which is used as a key in json object, like VMEM.
     *
     * @param commodityType commodity type
     * @return commodity type string used in json object
     */
    @Nullable
    public static String getCommodityTypeJsonKey(int commodityType) {
        MetricType metricType = CommodityTypeUtils.protoIntToDb(commodityType);
        return metricType == null ? null : metricType.getLiteral();
    }

    /**
     * Serialize the given object as a byte array.
     *
     * @param exportedObject the object to serialize
     * @return array of bytes
     * @throws JsonProcessingException error in serialization
     */
    public static byte[] toBytes(ExportedObject exportedObject) throws JsonProcessingException {
        return objectMapper.writeValueAsBytes(exportedObject);
    }

    /**
     * Convert the given byte array to an {@link ExportedObject}.
     *
     * @param bytes json byte array of an {@link ExportedObject}
     * @return an {@link ExportedObject}
     * @throws IOException error in json bytes deserialization
     */
    public static ExportedObject fromBytes(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, ExportedObject.class);
    }

    /**
     * Convert given {@link Tags} to a map from tag key to tag values list. Make sure the values
     * are sorted alphabetically, so that if the tags for the same entity appear in a
     * different order on a new EntityDTO we will still end up with the same hash code.
     *
     * @param tags {@link Tags}
     * @return map from tag key to tag values list
     */
    public static Map<String, List<String>> tagsToMap(Tags tags) {
        Map<String, List<String>> map = new HashMap<>();
        tags.getTagsMap().forEach((key, values) -> {
            map.computeIfAbsent(key, t -> new ArrayList<>())
                    .addAll(values.getValuesList()
                        .stream().sorted().collect(Collectors.toList()));
        });
        return map;
    }

    /**
     * Convert given {@link Tags} to a set of key value combinations. It combines tag key and value
     * using = as separator and put all combinations into a set like: ["owner=alex","owner=bob"].
     * If the value is empty, it just adds the key into the set.
     *
     * @param tags {@link Tags}
     * @return set of key value combinations
     */
    public static Set<String> tagsToKeyValueConcatSet(@Nonnull Tags tags) {
        Set<String> keyValueConcat = new HashSet<>();
        tags.getTagsMap().forEach((key, values) -> {
            List<String> valuesList = values.getValuesList();
            if (valuesList.isEmpty()) {
                keyValueConcat.add(key);
            } else {
                valuesList.forEach(value -> keyValueConcat.add(key + "=" + value));
            }
        });
        return keyValueConcat;
    }
}
