package com.vmturbo.extractor.export;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

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
     * Serialize the given {@link ExportedObject} collection as a byte array.
     *
     * @param objects collection of objects to serialize
     * @return array of bytes
     * @throws JsonProcessingException error in serialization
     */
    public static byte[] toBytes(Collection<ExportedObject> objects) throws JsonProcessingException {
        return objectMapper.writeValueAsBytes(objects);
    }

    /**
     * Convert the given byte array to a collection of {@link ExportedObject}s.
     *
     * @param bytes byte array of a collection of entities
     * @return collection of entities
     * @throws IOException error in json bytes deserialization
     */
    public static Collection<ExportedObject> fromBytes(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, new TypeReference<Collection<ExportedObject>>() {});
    }
}
