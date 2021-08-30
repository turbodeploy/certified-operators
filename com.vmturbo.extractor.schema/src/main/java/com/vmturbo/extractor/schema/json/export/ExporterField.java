package com.vmturbo.extractor.schema.json.export;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.jooq.EnumType;

/**
 * Annotation for the field in the POJO in data exporter, used for generating documentation.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface ExporterField {

    /**
     * Enum reference for the key of the map, if this field is a map. Use an array so empty means
     * not set, it's not allowed to assign default null here.
     *
     * @return enum for the key of the map field
     */
    Class<? extends EnumType>[] mapKeyEnum() default {};

    /**
     * Enum reference for the value of the field. Use an array so empty means not set, it's not
     * allowed to assign default null here.
     *
     * @return enum of the value
     */
    Class<? extends EnumType>[] valueEnum() default {};

    /**
     * Format of the string field, like the format for a timestamp field.
     *
     * @return string representing the format
     */
    String format() default "";

    /**
     * Whether this field is shared between data exporter and embedded reporting. If true, the
     * documentation for this field will be put under a shared section. If the field is a map, then
     * this is used for the value of the map.
     *
     * @return true if the field is shared, otherwise false
     */
    boolean shared() default false;

    /**
     * Whether the data for this field is extracted based on the metadata definition, like:
     * entity.attrs. It will not be inspected like other POJOs.
     *
     * @return true if the field is metadata.
     */
    boolean basedOnMetadata() default false;
}
