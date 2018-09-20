package com.vmturbo.identity.attributes;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStore;

/**
 * Capture the com.vmturbo.identity.attributes to implement a heuristics-based comparison for items
 * stored in an {@link IdentityStore}.
 *
 * The com.vmturbo.identity.attributes are partitioned into three categories:
 * <ul>
 *     <li>non-volatile - should never change
 *     <li>volatile - may change on rare occasion
 *     <li>heuristic - other com.vmturbo.identity.attributes that will resolve equality in case the non-volatile
 *     com.vmturbo.identity.attributes match and the volatile com.vmturbo.identity.attributes do not.
 * </ul>
 *
 * Note that for this class only the 'non-volatile' com.vmturbo.identity.attributes are included in the 'hashCode'
 * calculation, while the 'equals' implements the heuristic match which potentially involves
 * all three sets of com.vmturbo.identity.attributes. We guarantee that if two {@link HeuristicMatchingAttributes}
 * are 'equal()' then the 'hashCode()' values are also equal. This may increase the
 * number of collisions when stored in a HashMap, but is still corrrect.
 *
 * See the 'equals()', 'heuristicEquals()', and 'hashCode' methods below.
 **/
@Immutable
public class HeuristicMatchingAttributes implements IdentityMatchingAttributes {

    /**
     * these com.vmturbo.identity.attributes are key to the identity of the item; if they differ, the items differ
     */
    private final Set<IdentityMatchingAttribute> nonVolatileAttributes;
    /**
     * these com.vmturbo.identity.attributes are used to match but may change over time and then the 'heuristic' must be used
     */
    private final Set<IdentityMatchingAttribute> volatileAttributes;

    /**
     * for heuristic matching, use these com.vmturbo.identity.attributes and the threshold pct to handle 'equals'
     * if the nonVolatileAttributes match but the volatileAttributes do not
     */
    private final Set<IdentityMatchingAttribute> heuristicAttributes;
    /**
     * The matching threshold if heuristic matching is necessary
     */
    private final float heuristicThreshold;

    private final Set<IdentityMatchingAttribute> allAttributes;

    // If not specified, use 75% heuristic matching threshold
    private static final float HEURISTIC_THRESHOLD_DEFAULT = 0.75f;

    /**
     * Capture the three sets of com.vmturbo.identity.attributes for this item and the heuristic threshold.
     * The constructor is private - use the Builder instead, please.
     *
     * @param nonVolatileAttributes the com.vmturbo.identity.attributes that never change and must match to be equal
     * @param volatileAttributes the com.vmturbo.identity.attributes that may change; if they do, then must use heuristics
     * @param heuristicAttributes the com.vmturbo.identity.attributes for the heuristic match; match count must be > threshold
     * @param heuristicThreshold the threshold, 0-1, compared to the fraction heuristic com.vmturbo.identity.attributes
     *                           that match
     */
    private HeuristicMatchingAttributes(@Nonnull Set<IdentityMatchingAttribute> nonVolatileAttributes,
                                        @Nonnull Set<IdentityMatchingAttribute> volatileAttributes,
                                        @Nonnull Set<IdentityMatchingAttribute> heuristicAttributes,
                                        float heuristicThreshold) {
        this.nonVolatileAttributes = Objects.requireNonNull(nonVolatileAttributes);
        this.volatileAttributes = Objects.requireNonNull(volatileAttributes);
        this.heuristicAttributes = Objects.requireNonNull(heuristicAttributes);
        this.heuristicThreshold = heuristicThreshold;
        allAttributes = ImmutableSet.<IdentityMatchingAttribute>builder()
                .addAll(nonVolatileAttributes)
                .addAll(volatileAttributes)
                .addAll(heuristicAttributes)
                .build();
    }

    @Override
    @Nonnull
    public Set<IdentityMatchingAttribute> getMatchingAttributes() {
        return allAttributes;
    }

    @Nonnull
    @Override
    public IdentityMatchingAttribute getMatchingAttribute(String attributeId)
            throws IdentityStoreException {
        return allAttributes.stream()
                .filter(attribute -> attribute.getAttributeId().equals(attributeId))
                .findFirst().orElseThrow(() -> new IdentityStoreException(
                        "Error fetching IdentityMatchingAttribute: " + attributeId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof HeuristicMatchingAttributes)) return false;

        HeuristicMatchingAttributes that = (HeuristicMatchingAttributes) o;

        // note that 'equals' takes into account all the com.vmturbo.identity.attributes, but 'hashCode' only
        // depends on the nonVolatile com.vmturbo.identity.attributes. The nonVolatile com.vmturbo.identity.attributes must match,
        // and if the volatile com.vmturbo.identity.attributes don't match then the "heuristic" com.vmturbo.identity.attributes
        // come into play.
        return nonVolatileAttributes.equals(that.nonVolatileAttributes) &&
                (volatileAttributes.equals(that.volatileAttributes) ||
                        heuristicEquals(that));
    }

    /**
     * The "heuristicEquals" is invoked if the non-volatile com.vmturbo.identity.attributes match but the
     * volatile com.vmturbo.identity.attributes do not. Two heuristicAttributes sets are "heuristicEquals"
     * if the heuristicThreshold of both entities are the same and the percentage of heuristics
     * that match is greater that the heuristicThreshold.
     *
     * @param that the "other" HeuristicMatchingAttributes that is being tested for matching
     * @return true iff the heuristicThreshold values are equal and the percent of the two
     * heuristic values that match is greater to or equal that the heuristicThreshold.
     */
    private boolean heuristicEquals(HeuristicMatchingAttributes that) {
        if (heuristicThreshold != that.heuristicThreshold) {
            return false;
        }
        float overlapSize = Sets.intersection(heuristicAttributes,
                that.heuristicAttributes).size();
        return ((overlapSize / heuristicAttributes.size()) >= heuristicThreshold);
    }

    @Override
    public int hashCode() {
        // Note that the hashcode is based ONLY on the non-volatile com.vmturbo.identity.attributes. This will
        // lead to more frequent hash collisions, because 'equals' takes into account the
        // volatile and heuristic com.vmturbo.identity.attributes as well. Defining 'equals()' this way allows
        // us to use hashmap, for example, with an HeuristicMatchingAttributes instance as a key.
        return Objects.hashCode(nonVolatileAttributes);
    }

    /**
     * Instantiate a builder pattern for constructing a {@link HeuristicMatchingAttributes}
     * instance.
     *
     * @return a new Builder for constructing a {@link HeuristicMatchingAttributes} instance
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Implement a 'builder' pattern for constructing an {@link HeuristicMatchingAttributes}
     * instance.
     */
    @NotThreadSafe
    public static class Builder {

        private Set<IdentityMatchingAttribute> nonVolatileAttributes = Collections.emptySet();
        private Set<IdentityMatchingAttribute> volatileAttributes = Collections.emptySet();
        private Set<IdentityMatchingAttribute> heuristicAttributes = Collections.emptySet();
        private float heuristicThreshold = HEURISTIC_THRESHOLD_DEFAULT;

        /**
         * Replace the primary matching com.vmturbo.identity.attributes with the given set of
         * {@link IdentityMatchingAttribute}.
         *
         * @param nonVolatileAttributes the set of primary matchinging com.vmturbo.identity.attributes to store
         * @return this builder instance
         */
        public Builder setNonVolatileAttributes(Set<IdentityMatchingAttribute> nonVolatileAttributes) {
            this.nonVolatileAttributes = nonVolatileAttributes;
            return this;
        }

        /**
         * Replace the secondary matching com.vmturbo.identity.attributes with the given set of
         * {@link IdentityMatchingAttribute}.
         *
         * @param volatileAttributes the set of secondary matchinging com.vmturbo.identity.attributes to store
         * @return this builder instance
         */
        public Builder setVolatileAttributes(Set<IdentityMatchingAttribute> volatileAttributes) {
            this.volatileAttributes = volatileAttributes;
            return this;
        }

        /**
         * Replace the heuristic matching com.vmturbo.identity.attributes with the given set of
         * {@link IdentityMatchingAttribute}, used if the nonVolatile com.vmturbo.identity.attributes match but the
         * volatile com.vmturbo.identity.attributes do not match.
         *
         * @param heuristicAttributes the set of heuristic matchinging com.vmturbo.identity.attributes to store
         * @return this builder instance
         */
        public Builder setHeuristicAttributes(Set<IdentityMatchingAttribute> heuristicAttributes) {
            this.heuristicAttributes = heuristicAttributes;
            return this;
        }

        /**
         * A matching threshold used if the nonVolatile com.vmturbo.identity.attributes match but the volatile com.vmturbo.identity.attributes
         * do not match. If the fraction of heuristic com.vmturbo.identity.attributes that match between two items
         * is greater or equal to the heuristicThreshold, then the items are considered the same.
         *
         * @param heuristicThreshold the threshold value to use when comparing the fraction of
         *                           matching heuristic com.vmturbo.identity.attributes between two items
         * @return this builder instance
         */
        public Builder setHeuristicThreshold(float heuristicThreshold) {
            this.heuristicThreshold = heuristicThreshold;
            return this;
        }

        /**
         * Build a new instance of {@link HeuristicMatchingAttributes} from this builder's values.
         *
         * @return a new instance of {@link HeuristicMatchingAttributes} built from this builder's values
         */
        public HeuristicMatchingAttributes build() {
            return new HeuristicMatchingAttributes(nonVolatileAttributes, volatileAttributes,
                    heuristicAttributes, heuristicThreshold);
        }

    }
}
