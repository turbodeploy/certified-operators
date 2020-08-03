package com.vmturbo.reserved.instance.coverage.allocator.key;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;

/**
 * A key in {@link CoverageKeyRepository}. The key is meant to be agnostic to the represented data type,
 * containing only relevant key material for mapping RIs to entities.
 */
@Immutable
public interface CoverageKey {

    /**
     * @return The key material of this {@link CoverageKey}. Equality between two coverage keys is
     * synonymous with {@link Map#equals(Object)}.
     */
    @Nonnull
    Map<String, Object> getKeyMaterial();

    /**
     * A builder class for {@link CoverageKey}
     */
    interface Builder {

        /**
         * Add key material to this builder
         * @param key The identifier of the key material
         * @param value The value of the key material
         * @return The instance of {@link CloudProviderCoverageContext.Builder} for method chaining
         */
        @Nonnull
        Builder keyMaterial(@Nonnull String key, @Nonnull Object value);

        /**
         * @return A newly created instance of {@link CoverageKey}
         */
        @Nonnull
        CoverageKey build();
    }
}
