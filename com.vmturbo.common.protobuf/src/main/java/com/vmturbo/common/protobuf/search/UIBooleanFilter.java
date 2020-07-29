package com.vmturbo.common.protobuf.search;

import javax.annotation.Nonnull;

/**
 * This represents the API/UI options for "boolean" filters.
 */
public enum UIBooleanFilter {

    /**
     * Accepts the filter criteria.
     */
    TRUE("True"),

    /**
     * Does not accept the filter criteria.
     */
    FALSE("False");

    private final String value;

    UIBooleanFilter(@Nonnull final String value) {
        this.value = value;
    }

    /**
     * Get the string value of the enum options. This is the value that the UI/API works with.
     *
     * @return The string value.
     */
    @Nonnull
    public String apiStr() {
        return value;
    }
}