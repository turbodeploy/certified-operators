package com.vmturbo.identity.attributes;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * One attribute to match on, defined by an 'attributeId' and an 'attributeValue'.
 */
@Immutable
public class IdentityMatchingAttribute {

    /**
     *  A String id for this attribute
     */
    private final String attributeId;
    /**
     * The String value for this attribute
     */
    private final String attributeValue;

    /**
     * Create a new instance of this {@link IdentityMatchingAttribute}.
     *
     * @param attributeId the ID string for the new attribute
     * @param attributeValue the value of this attribute
     */
    public IdentityMatchingAttribute(@Nonnull String attributeId, @Nonnull String attributeValue) {
        this.attributeId = Objects.requireNonNull(attributeId);
        this.attributeValue = Objects.requireNonNull(attributeValue);
    }

    public String getAttributeId() {
        return attributeId;
    }

    public String getAttributeValue() {
        return attributeValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IdentityMatchingAttribute)) return false;

        IdentityMatchingAttribute that = (IdentityMatchingAttribute) o;

        // both the attributeId and attributeValue must match
        return attributeId.equals(that.attributeId) &&
                attributeValue.equals(that.attributeValue);
    }

    @Override
    public int hashCode() {
        // combine the has value of both the attributeId and attributeValue together
        return Objects.hash(attributeId, attributeValue);
    }

    @Override
    public String toString() {
        return String.format("%s : %s ", attributeId, attributeValue);
    }

}
