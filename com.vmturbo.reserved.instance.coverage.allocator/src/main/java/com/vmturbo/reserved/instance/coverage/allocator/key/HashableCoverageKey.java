package com.vmturbo.reserved.instance.coverage.allocator.key;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableMap;

/**
 * An implemenation of {@link CoverageKey} in which equality is enforced through hashing
 * of the {@link CoverageKey#getKeyMaterial()}.
 */
@Immutable
public class HashableCoverageKey implements CoverageKey {

    private final Map<String, Object> keyMaterial;

    private HashableCoverageKey(@Nonnull Builder builder) {
        this.keyMaterial = ImmutableMap.copyOf(builder.keyMaterial);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object other) {
        return other != null && other instanceof CoverageKey &&
                this.getKeyMaterial().equals(((CoverageKey)other).getKeyMaterial());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return getKeyMaterial().hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<String, Object> getKeyMaterial() {
        return keyMaterial;
    }

    /**
     * @return A new {@link Builder} instance
     */
    public static CoverageKey.Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder class for {@link HashableCoverageKey}
     */
    public static class Builder implements CoverageKey.Builder {

        private final Map<String, Object> keyMaterial = new HashMap<>();

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public CoverageKey.Builder keyMaterial(@Nonnull final String key, @Nonnull final Object value) {
            keyMaterial.put(key, value);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public CoverageKey build() {
            return new HashableCoverageKey(this);
        }
    }
}
