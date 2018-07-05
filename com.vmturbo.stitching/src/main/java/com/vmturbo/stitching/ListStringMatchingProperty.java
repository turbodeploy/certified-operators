package com.vmturbo.stitching;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

/**
 * An entity property that returns a list of Strings.  Since entity properties are only Strings,
 * these require a delimiter to parse out the individual strings.
 */
public class ListStringMatchingProperty extends MatchingProperty<List<String>> {

    // delimiter for separating individual strings within a single entity property
    private final String delimiter;

    /**
     * Constructor for a matcher that matches a property that is a list of strings delimited by the
     * given delimiter.
     *
     * @param propertyName The name of the property that holds the list of strings
     * @param delimiter The delimiter that separates the strings within the property
     */
    public ListStringMatchingProperty(@Nonnull String propertyName, @Nonnull String delimiter) {
        super(propertyName);
        this.delimiter = delimiter;
    }

    @Override
    public Optional<List<String>> parse(final String rawProperty) {
        return Optional.of(Lists.newArrayList(rawProperty.split(delimiter)));
    }
}