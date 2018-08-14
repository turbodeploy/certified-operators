package com.vmturbo.identity.attributes;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Sets;

/**
 * A simple set of attributes, consisting of key/value pairs. Defines 'equals' to be
 * equals on the underlying sets.
 **/
@Immutable
public class SimpleMatchingAttributes implements IdentityMatchingAttributes {

    /**
     * the Set of {@link IdentityMatchingAttributes} we are storing
     */
    private final Set<IdentityMatchingAttribute> attributes;

    /**
     * Private constructor called by the Builder below.
     *
     * @param attributes the {@link Set} of {@link IdentityMatchingAttribute} that define
     *                   this collection.
     */
    private SimpleMatchingAttributes(@Nonnull Set<IdentityMatchingAttribute> attributes) {
        Objects.requireNonNull(attributes);
        this.attributes = Collections.unmodifiableSet(attributes);
    }

    /**
     * Return the {@link Set} of {@link IdentityMatchingAttribute} that make up this collection.
     *
     * @return the {@link Set} of {@link IdentityMatchingAttribute} objects for this collection
     */
    @Override
    @Nonnull
    public Set<IdentityMatchingAttribute> getMatchingAttributes() {
        return attributes;
    }

    /**
     * The 'equals()' method uses the underlying 'attributes' Set. This depends on 'equals()'
     * being defined for the {@link IdentityMatchingAttribute} class.
     *
     * @param o the 'other' object to compare with this one
     * @return true iff the 'attributes' for this item equals the 'attributes' for the other.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleMatchingAttributes)) return false;

        SimpleMatchingAttributes that = (SimpleMatchingAttributes) o;

        return attributes.equals(that.attributes);
    }

    /**
     * The 'hashCode()' method returns the hash of the 'attributes' Set.
     * This depends on 'hashCode()' being defined for the {@link IdentityMatchingAttribute} class.
     *
     * @return the hash computed over elements of the 'attributes' Set
     */
    @Override
    public int hashCode() {
        return attributes.hashCode();
    }

    /**
     * Return a Builder for a SimpleMatchingAttributes object.
     *
     * @return a Builder to collect the {@link IdentityMatchingAttribute} objects for this
     * {@link SimpleMatchingAttributes}
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Implement a Builder pattern for a {@link SimpleMatchingAttributes} object.
     */
    @NotThreadSafe
    public static class Builder {

        // the {@link IdentityMatchingAttribute} set we are accumulating
        private Set<IdentityMatchingAttribute> attributes = Sets.newHashSet();

        /**
         * Construct an {@link IdentityMatchingAttribute} object from the given attributeId and
         * attributeValue and add that new object to the Set for this builder.
         *
         * @param attrId the String ID of the attribute
         * @param attrValue the String Value of the attribute
         * @return this builder instance
         */
        @Nonnull
        public Builder addAttribute(@Nonnull String attrId, @Nonnull String attrValue) {
            attributes.add(new IdentityMatchingAttribute(Objects.requireNonNull(attrId),
                    Objects.requireNonNull(attrValue)));
            return this;
        }

        /**
         * Add the set of {@link IdentityMatchingAttribute} objects given to the Set for this builder.
         *
         * @param attributes a Set of {@link IdentityMatchingAttribute} object to add
         * @return this builder instance
         */
        @Nonnull
        public Builder addAttributes(Set<IdentityMatchingAttribute> attributes) {
            this.attributes.addAll(attributes);
            return this;
        }

        /**
         * Instantiate a new instance of {@link SimpleMatchingAttributes} based on the build values.
         *
         * @return a new instance of {@link SimpleMatchingAttributes} based on the build values
         */
        @Nonnull
        public SimpleMatchingAttributes build() {
            return new SimpleMatchingAttributes(attributes);
        }
    }
}
