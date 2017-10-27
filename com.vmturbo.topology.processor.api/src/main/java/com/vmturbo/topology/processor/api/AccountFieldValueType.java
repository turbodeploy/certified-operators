package com.vmturbo.topology.processor.api;

/**
 * Type of account value field.
 */
public enum AccountFieldValueType {
    /**
     * Primitive value represents string value and should be used "as is".
     */
    STRING,
    /**
     * Primitive value represents boolean value and should have values only {@code "true"} or
     * {@code "false"}.
     */
    BOOLEAN,
    /**
     * Primitive value represents numeric value and should have value, contains only digits (whole
     * numeric, not-fractional).
     */
    NUMERIC,
    /**
     * Value represents list field. {@code value} is used only in
     * {@link SettingApiDTO} but not in {@link InputFieldApiDTO}
     */
    LIST,
    /**
     * Value represents group scope field. {@code value} is not used in {@link InputFieldApiDTO},
     * instead {@code groupProperties} is used.
     */
    GROUP_SCOPE;

}
