package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import jdk.nashorn.internal.ir.annotations.Immutable;

/**
 * A configuration for {@link ConfigurableRICoverageRule}
 */
@Immutable
public class RICoverageRuleConfig {

    private final boolean isSharedScope;
    private final boolean isZoneScoped;
    private final Boolean isSizeFlexible;
    private final Boolean isPlatformFlexible;

    private RICoverageRuleConfig(@Nonnull Builder builder) {
        this.isSharedScope = builder.isSharedScope;
        this.isZoneScoped = builder.isZoneScoped;
        this.isSizeFlexible = builder.isSizeFlexible;
        this.isPlatformFlexible = builder.isPlatformFlexible;
    }

    /**
     * @return True, if shared scope should be used for the rule (comparing billing family/EA family).
     * False otherwise
     */
    public boolean isSharedScope() {
        return isSharedScope;
    }

    /**
     * @return True, if the connected availability zone should be used for key comparison
     */
    public boolean isZoneScoped() {
        return isZoneScoped;
    }

    /**
     * @return An optional containing instance size flexibility. If present, only RIs matching the
     * value will be applicable to this rule. If not present, no RIs will be filtered based on
     * size flexibility.
     */
    public Optional<Boolean> isSizeFlexible() {
        return Optional.ofNullable(isSizeFlexible);
    }

    /**
     * @return An optional containing platform flexibility. If present, only RIs matching the
     * value will be applicable to this rule. If not present, no RIs will be filtered based on
     * platform flexibility.
     */
    public Optional<Boolean> isPlatformFlexible() {
        return Optional.ofNullable(isPlatformFlexible);
    }

    /**
     * @return A new instance of {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link RICoverageRuleConfig}
     */
    public static class Builder {


        private boolean isSharedScope;
        private boolean isZoneScoped;
        private Boolean isSizeFlexible;
        private Boolean isPlatformFlexible;

        /**
         * Set the shared scope flag
         * @param isSharedScope The boolean flag
         * @return This instance of {@link Builder} for method chaining
         */
        public Builder isSharedScope(boolean isSharedScope) {
            this.isSharedScope = isSharedScope;
            return this;
        }

        /**
         * Set the zone scoped flag
         * @param isZoneScoped The boolean flag
         * @return This instance of {@link Builder} for method chaining
         */
        public Builder isZoneScoped(boolean isZoneScoped) {
            this.isZoneScoped = isZoneScoped;
            return this;
        }

        /**
         * Set the size flexible flag. Passing null will configure the associated rule to
         * ignore size flexibility
         * @param isSizeFlexible The boolean flag or null
         * @return This instance of {@link Builder} for method chaining
         */
        public Builder isSizeFlexible(@Nullable Boolean isSizeFlexible) {
            this.isSizeFlexible = isSizeFlexible;
            return this;
        }

        /**
         * Set the platform flexible flag. Passing null will configure the associated rule to
         * ignore platform flexibility
         * @param isPlatformFlexible The boolean flag or null
         * @return This instance of {@link Builder} for method chaining
         */
        public Builder isPlatformFlexible(@Nullable Boolean isPlatformFlexible) {
            this.isPlatformFlexible = isPlatformFlexible;
            return this;
        }

        /**
         * @return A newly created instance of {@link RICoverageRuleConfig}, based on this builder
         */
        public RICoverageRuleConfig build() {
            return new RICoverageRuleConfig(this);
        }
    }
}
