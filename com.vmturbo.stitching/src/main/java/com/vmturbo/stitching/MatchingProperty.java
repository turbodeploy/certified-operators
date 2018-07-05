package com.vmturbo.stitching;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;

/**
 * A class that represents a property of an entity used for matching.  Since entity properties
 * may be more complex types represented as strings, we have an abstract method parse
 * which subclasses must implement to parse the String into the actual value that should be used
 * for matching.
 *
 * @param <RETURN_TYPE> the type of value the matching property represents.
 */
public abstract class MatchingProperty <RETURN_TYPE> implements
        MatchingPropertyOrField <RETURN_TYPE> {

    final private String propertyName;

    public MatchingProperty(@Nonnull String propertyName) {
        this.propertyName = propertyName;
    }

    /**
     * Parse the string value of the property and return the actual value it represents.
     *
     * @param rawProperty the property as a string
     * @return the converted property
     */
    public abstract Optional<RETURN_TYPE> parse(String rawProperty);

    @Override
    public Optional<RETURN_TYPE> getMatchingValue(final StitchingEntity entity) {
        if (propertyName != null) {
            Optional<String> propVal = entity.getEntityBuilder().getEntityPropertiesList().stream()
                    .filter(ep -> propertyName.equals(ep.getName())).map(EntityProperty::getValue)
                    .findFirst();
            if (propVal.isPresent()) {
                return parse(propVal.get());
            }
        }
        return Optional.empty();
    }
}
