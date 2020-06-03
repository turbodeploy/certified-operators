package com.vmturbo.stitching;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;

/**
 * A class that represents a property of an entity used for matching.  Since entity properties may
 * be more complex types represented as strings, we have an abstract method parse which subclasses
 * must implement to parse the String into the actual value that should be used for matching.
 */
public class MatchingProperty implements MatchingPropertyOrField<String> {
    private final String propertyName;
    private final String delimiter;

    /**
     * Create {@link MatchingProperty} instance.
     *
     * @param propertyName name of the property which value will be extracted.
     * @param delimiter delimiter that might be used to separate individual values
     *                 in one string value.
     */
    public MatchingProperty(@Nonnull String propertyName, @Nullable String delimiter) {
        this.propertyName = propertyName;
        this.delimiter = delimiter;
    }

    /**
     * Create {@link MatchingProperty} instance.
     *
     * @param propertyName name of the property which value will be extracted.
     */
    public MatchingProperty(@Nonnull String propertyName) {
        this(propertyName, null);
    }

    /**
     * Parse the string value of the property and return the actual value it represents.
     *
     * @param rawProperty the property as a string
     * @return the converted property
     */
    @Nonnull
    private Collection<String> parse(@Nonnull Collection<String> rawProperty) {
        if (StringUtils.isEmpty(delimiter)) {
            return rawProperty;

        }
        return rawProperty.stream()
                        .map(item -> item.split(delimiter))
                        .map(Lists::newArrayList)
                        .flatMap(Collection::stream)
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public Collection<String> getMatchingValue(@Nonnull StitchingEntity entity) {
        final Collection<String> values =
                        entity.getEntityBuilder().getEntityPropertiesList().stream()
                                        .filter(ep -> Objects.equals(propertyName, ep.getName()))
                                        .map(EntityProperty::getValue)
                                        .filter(StringUtils::isNotBlank)
                                        .collect(Collectors.toSet());
        return parse(values);
    }
}
