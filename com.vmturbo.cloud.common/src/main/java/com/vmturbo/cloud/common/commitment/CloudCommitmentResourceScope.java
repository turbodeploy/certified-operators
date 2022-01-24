package com.vmturbo.cloud.common.commitment;

import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

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
    interface ComputeTierResourceScope extends CloudCommitmentResourceScope {

        /**
         * {@inheritDoc}.
         */
        @Derived
        @Override
        default Set<EntityType> cloudTiers() {
            return ImmutableSet.of(EntityType.COMPUTE_TIER);
        }

        /**
         * The compute tier family.
         * @return The compute tier family.
         */
        @Nonnull
        String computeTierFamily();

        /**
         * The platform info.
         * @return The platform info.
         */
        @Nonnull
        PlatformInfo platformInfo();

        /**
         * The set of supported tenancies.
         * @return The set of supported tenancies.
         */
        @Nonnull
        Set<Tenancy> tenancies();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing immutable {@link ComputeTierResourceScope} instances.
         */
        class Builder extends ImmutableComputeTierResourceScope.Builder {}

        /**
         * The platform information.
         */
        @HiddenImmutableImplementation
        @Immutable
        interface PlatformInfo {

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
