package com.vmturbo.stitching;

import java.util.Optional;

/**
 * A class for matching properties that are type String.
 */
public class StringMatchingProperty extends MatchingProperty<String> {

    public StringMatchingProperty(String propertyName) {
        super(propertyName);
    }

    @Override
    public Optional<String> parse(final String rawProperty) {
        return Optional.of(rawProperty);
    }
}
