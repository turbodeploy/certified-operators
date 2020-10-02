package com.vmturbo.reserved.instance.coverage.allocator.key;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A configuration for {@link CoverageKeyCreator}, used in extracting key material for a
 * {@link CoverageKey} from either a {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought}
 * or {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO} instance
 */
@Immutable
public class CoverageKeyCreationConfig {

    private final boolean isSharedScope;

    private final boolean isPlatformFlexible;

    private final boolean isInstanceSizeFlexible;

    private final boolean isZoneScoped;

    private CoverageKeyCreationConfig(Builder builder) {
        this.isSharedScope = builder.isSharedScope;
        this.isPlatformFlexible = builder.isPlatformFlexible;
        this.isInstanceSizeFlexible = builder.isInstanceSizeFlexible;
        this.isZoneScoped = builder.isZoneScoped;
    }

    /**
     * @return True, if the billing family scope should be used. False, if the direct account
     * owner should be used
     */
    public boolean isSharedScope() {
        return isSharedScope;
    }

    /**
     * @return True, if the OSType of the RI/entity should be ignored. False, if the OSType should
     * be added to key material of a {@link CoverageKey}
     */
    public boolean isPlatformFlexible() {
        return isPlatformFlexible;
    }

    /**
     * @return True, if only instance family should be used as key material. If false, the specific
     * instance type associated with the RI or entity will be used as key material
     */
    public boolean isInstanceSizeFlexible() {
        return isInstanceSizeFlexible;
    }

    /**
     * @return True, if the zone connected to the RI or entity should be used as key material. If false,
     * only the connected region will be used as key material.
     */
    public boolean isZoneScoped() {
        return isZoneScoped;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isSharedScope, isZoneScoped, isInstanceSizeFlexible, isPlatformFlexible);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof CoverageKeyCreationConfig) {
            final CoverageKeyCreationConfig otherConfig = (CoverageKeyCreationConfig)obj;
            return isSharedScope == otherConfig.isSharedScope &&
                    isZoneScoped == otherConfig.isZoneScoped &&
                    isInstanceSizeFlexible == otherConfig.isInstanceSizeFlexible &&
                    isPlatformFlexible == otherConfig.isPlatformFlexible;
        }

        return false;
    }

    /**
     * @return A new instance of {@link Builder}
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder class for {@link CoverageKeyCreationConfig}
     */
    public static class Builder {

        private boolean isSharedScope = false;
        private boolean isPlatformFlexible = false;
        private boolean isInstanceSizeFlexible = false;
        private boolean isZoneScoped = false;

        /**
         * Set the shared scope flag
         * @param isSharedScope The boolean flag
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public Builder isSharedScope(boolean isSharedScope) {
            this.isSharedScope = isSharedScope;
            return this;
        }

        /**
         * Set the platform flexible flag
         * @param isPlatformFlexible The boolean flag
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public Builder isPlatformFlexible(boolean isPlatformFlexible) {
            this.isPlatformFlexible = isPlatformFlexible;
            return this;
        }

        /**
         * Set the instance size flexible flag
         * @param isInstanceSizeFlexible The boolean flag
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public Builder isInstanceSizeFlexible(boolean isInstanceSizeFlexible) {
            this.isInstanceSizeFlexible = isInstanceSizeFlexible;
            return this;
        }

        /**
         * Set the zonal scoping flag
         * @param isZoneScoped The boolean flag
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public Builder isZoneScoped(boolean isZoneScoped) {
            this.isZoneScoped = isZoneScoped;
            return this;
        }

        /**
         * @return A newly created instance of {@link CoverageKeyCreationConfig}
         */
        @Nonnull
        public CoverageKeyCreationConfig build() {
            return new CoverageKeyCreationConfig(this);
        }
    }
}
