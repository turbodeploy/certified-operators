package com.vmturbo.cloud.common.commitment;

import java.util.OptionalLong;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * The resource scope (cloud services, cloud tiers, etc.) a cloud commitment can cover.
 */
@HiddenImmutableImplementation
@Immutable
public interface CloudCommitmentResourceScope {

    /**
     * The set of coverable cloud service OIDs.
     * @return The set of coverable cloud service OIDs.
     */
    @Nonnull
    Set<Long> cloudServices();

    /**
     * The set of the coverable entity types. If empty, the commitment can cover any entity type.
     * @return The set of the coverable entity types.
     */
    @Nonnull
    Set<EntityType> coveredEntityTypes();

    /**
     * The set of coverable cloud tier types.. If empty, the commitment can cover resources provided
     * by any cloud tier type.
     * @return The set of coverable cloud tiers types.
     */
    @Nonnull
    Set<EntityType> cloudTiers();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutable {@link CloudCommitmentResourceScope} instances.
     */
    class Builder extends ImmutableCloudCommitmentResourceScope.Builder {}

    /**
     * A resource scope for compute tier resources.
     */
    @HiddenImmutableImplementation
    @Immutable
    abstract class ComputeTierResourceScope implements CloudCommitmentResourceScope {

        /**
         * {@inheritDoc}.
         */
        @Derived
        @Override
        public Set<EntityType> cloudTiers() {
            return ImmutableSet.of(EntityType.COMPUTE_TIER);
        }

        /**
         * The compute tier family.
         * @return The compute tier family.
         */
        @Nullable
        public abstract String computeTierFamily();

        /**
         * The compute tier OID, if the commitments are scoped to a single tier.
         * @return The compute tier OID, if the commitments are scoped to a single tier.
         */
        @Nonnull
        public abstract OptionalLong computeTier();

        /**
         * True, if {@link #computeTierFamily()} is set, indicating {@link #computeTier()} is not set. False,
         * if {@link #computeTier()} is set.
         * @return True, if {@link #computeTierFamily()} is set, indicating {@link #computeTier()} is not set. False,
         * if {@link #computeTier()} is set.
         */
        @Derived
        public boolean isSizeFlexible() {
            return StringUtils.isNotBlank(computeTierFamily());
        }

        /**
         * The platform info.
         * @return The platform info.
         */
        @Nonnull
        public abstract PlatformInfo platformInfo();

        /**
         * The set of supported tenancies.
         * @return The set of supported tenancies.
         */
        @Nonnull
        public abstract Set<Tenancy> tenancies();

        @Check
        protected void validate() {

            Preconditions.checkState(isSizeFlexible() ^ computeTier().isPresent(),
                    "Either compute tier family or compute tier must be set");
        }

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        public static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing immutable {@link ComputeTierResourceScope} instances.
         */
        public static class Builder extends ImmutableComputeTierResourceScope.Builder {}

        /**
         * The platform information.
         */
        @HiddenImmutableImplementation
        @Immutable
        public interface PlatformInfo {

            /**
             * Indicates whether the cloud commitment can cover any OS.
             * @return Indicates whether the cloud commitment can cover any OS.
             */
            boolean isPlatformFlexible();

            /**
             * If {@link #isPlatformFlexible()} is false, indicates which specific platform
             * can be covered.
             * @return The coverable platform, if the commitment is not platform flexible.
             */
            @Default
            @Nonnull
            default OSType platform() {
                return OSType.UNKNOWN_OS;
            }

            /**
             * Constructs and returns a new {@link Builder} instance.
             * @return The newly constructed {@link Builder} instance.
             */
            @Nonnull
            static Builder builder() {
                return new Builder();
            }

            /**
             * A builder class for constructing immutable {@link PlatformInfo} instances.
             */
            class Builder extends ImmutablePlatformInfo.Builder {}
        }
    }
}
