package com.vmturbo.common.api.mappers;

import java.util.EnumSet;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

/**
 * Implementation for use with Enum keys.
 * Provides matching of keys to enumType.
 *
 * @param <E> enumType to which EnumMapper is implemented against
 */
public class EnumMapper<E extends Enum<E>> {

    private final Class<E> enumType;

    /**
     * Constructor for EnumMapper
     *
     * @param enumType Enum for which mapper is implemented against
     */
    public EnumMapper(final Class<E> enumType) {
        this.enumType = enumType;
    }

    /**
     * Returns Enum const matched to description.
     *
     * @param description Enum const to match against
     * @return Optional of matching enum constant
     */
    public Optional<E> valueOf(Enum description) {
        if (description == null) {
            return Optional.empty();
        }
        return valueOf(description.name());
    }

    /**
     * Returns Enum const matched to description.
     *
     * @param description Enum name to match
     * @return Optional of matching enum constant
     */
    public Optional<E> valueOf(String description) {
        if (StringUtils.isBlank(description)) {
            return Optional.empty();
        }
        return EnumSet.allOf(enumType).stream()
                .filter(enumValue -> isMatch(enumValue, description))
                .findFirst();
    }

    /**
     * Matches enum const to description
     *
     * @param enumValue Enum const
     * @param description name of enum
     * @return boolen of weather arguments are found to match
     */
    private boolean isMatch(E enumValue, String description) {
        final String normalizedDescription = StringUtils.trimToEmpty(description).replaceAll("\\s", "_");
        return StringUtils.equalsIgnoreCase(enumValue.name(), normalizedDescription);
    }

    /**
     * Returns EnumMapper of specified enum Type
     *
     * @param enumType the Class object of the enum type from which to return a constant
     * @param <T>Enum type whose const is to be returned
     * @return
     */
    public static <T extends Enum<T>> EnumMapper<T> of(Class<T> enumType) {
        return new EnumMapper<>(enumType);
    }
}
