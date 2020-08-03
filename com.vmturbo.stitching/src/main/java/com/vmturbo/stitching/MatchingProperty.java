package com.vmturbo.stitching;

import java.util.Collection;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;

/**
 * A class that represents a property of an entity used for matching.  Since entity properties may
 * be more complex types represented as strings, we have an abstract method parse which subclasses
 * must implement to parse the String into the actual value that should be used for matching.
 */
public class MatchingProperty implements MatchingPropertyOrField<String> {
    private final String propertyName;
    private final Pattern splitter;

    /**
     * Create {@link MatchingProperty} instance.
     *
     * @param propertyName name of the property which value will be extracted.
     * @param splitter splitter that might be used to separate individual values
     *                 in one string value.
     */
    public MatchingProperty(@Nonnull String propertyName, @Nullable String splitter) {
        this.propertyName = propertyName;
        this.splitter = splitter == null ? null : Pattern.compile(splitter);
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
        if (splitter == null) {
            return rawProperty;

        }
        return rawProperty.stream()
                        .map(splitter::split)
                        .map(Sets::newHashSet)
                        .flatMap(Collection::stream)
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public Collection<String> getMatchingValue(@Nonnull StitchingEntity entity) {
        return parse(entity.getPropertyValues(propertyName));
    }

    @Override
    public String toString() {
        return String.format("%s [propertyName=%s, delimiter=%s]", getClass().getSimpleName(),
                        this.propertyName, splitter == null ? null : splitter.pattern());
    }
}
