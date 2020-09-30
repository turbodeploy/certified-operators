package com.vmturbo.cloud.common.commitment.aggregator;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Aggregation info specific to reserved instances.
 */
@HiddenImmutableImplementation
@Immutable(lazyhash = true)
public interface ReservedInstanceAggregateInfo extends AggregateInfo {

    /**
     * The platform info of the aggregate.
     * @return The platform info of the aggregate.
     */
    @Nonnull
    PlatformInfo platformInfo();

    /**
     * The tier info of the aggregate.
     * @return The tier info of the aggregate.
     */
    @Nonnull
    TierInfo tierInfo();

    /**
     * The scope info of the aggregate.
     * @return The scope info of the aggregate.
     */
    @Nonnull
    ReservedInstanceScopeInfo scopeInfo();

    /**
     * The region OID of the aggregate.
     * @return The region OID of the aggregate.
     */
    long regionOid();

    /**
     * The zone OID of the aggregate.
     * @return The zone OID of the aggregate, if one is set.
     */
    @Nonnull
    OptionalLong zoneOid();

    /**
     * The tenancy of the aggregate. If none is set, defaults to {@link Tenancy#DEFAULT}.
     * @return The tenancy of the aggregate. If none is set, defaults to {@link Tenancy#DEFAULT}.
     */
    @Nonnull
    @Default
    default Tenancy tenancy() {
        return Tenancy.DEFAULT;
    }

    /**
     * The commitment type of the aggregate. This will always return {@link CloudCommitmentType#RESERVED_INSTANCE}.
     * @return The commitment type of the aggregate. This will always return {@link CloudCommitmentType#RESERVED_INSTANCE}.
     */
    @Nonnull
    @Derived
    default CloudCommitmentType commitmentType() {
        return CloudCommitmentType.RESERVED_INSTANCE;
    }

    /**
     * Constructs and return a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link ReservedInstanceAggregateInfo}.
     */
    class Builder extends ImmutableReservedInstanceAggregateInfo.Builder {}

    /**
     * A data class for platform (OS) info of an RI aggregate.
     */
    @HiddenImmutableImplementation
    @Immutable(lazyhash = true)
    abstract class PlatformInfo {

        /**
         * Whether the RI is platform flexible.
         * @return Whether the RI is platform flexible.
         */
        public abstract boolean isPlatformFlexible();

        /**
         * The platform of the RI. Defaults to {@link OSType#UNKNOWN_OS}.
         * @return The platform of the RI.
         */
        @Default
        public OSType platform() {
            return OSType.UNKNOWN_OS;
        }

        /**
         * Overrides the default hashcode, allowing two instances in which both are platform
         * flexible to be equal, regardless of the {@link #platform()}.
         * @return The hashcode of this instance.
         */
        @Override
        public int hashCode() {
            if (isPlatformFlexible()) {
                return Objects.hash(isPlatformFlexible());
            } else {
                return Objects.hash(isPlatformFlexible(), platform());
            }
        }

        /**
         * Overrides equality, allowing two instances in which both are platform flexible to
         * be considered equal.
         * @param obj The other instance two compare.
         * @return True, if both platform info instances are platform flexible or if the platform
         * is the same (if both are platform inflexible).
         */
        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof PlatformInfo) {
                final PlatformInfo otherInfo = (PlatformInfo)obj;
                return (isPlatformFlexible() == otherInfo.isPlatformFlexible())
                        && (isPlatformFlexible() || platform() == otherInfo.platform());
            } else {
                return false;
            }
        }

        /**
         * Creates and returns a new {@link Builder} instance.
         * @return A newly constructed builder instance.
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link PlatformInfo}.
         */
        public static class Builder extends ImmutablePlatformInfo.Builder {}
    }

    /**
     * A data class for tier information of an RI aggregate.
     */
    @HiddenImmutableImplementation
    @Immutable(lazyhash = true)
    abstract class TierInfo {

        private static final Set<EntityType> TIER_ENTITY_TYPES = ImmutableSet.of(
                EntityType.COMPUTE_TIER,
                EntityType.DATABASE_SERVER_TIER,
                EntityType.DATABASE_TIER,
                EntityType.STORAGE_TIER);

        /**
         * The tier type.
         * @return The tier type
         */
        @Nonnull
        public abstract EntityType tierType();

        /**
         * The tier's family. A tier will only be considered to have a family, if its normalization
         * factor (coupon value) is a positive value.
         * @return The tier's family.
         */
        @Nonnull
        public abstract Optional<String> tierFamily();

        /**
         * The tier oid.
         * @return The tier oid.
         */
        public abstract long tierOid();

        /**
         * Whether the RI aggregate is size flexible.
         * @return Whether the RI aggregate is size flexible.
         */
        public abstract boolean isSizeFlexible();

        /**
         * Overrides the default hashcode, allowing two RIs in which both are size flexible to
         * be compared by the family and tier type only.
         * @return The tier info hash code.
         */
        @Override
        public int hashCode() {
            if (isSizeFlexible()) {
                return Objects.hash(tierType(), tierFamily());
            } else {
                return Objects.hash(tierType(), tierType(), tierOid(), isSizeFlexible());
            }
        }

        /**
         * Overrides the default equals, supporting size flexibility comparison.
         * @param obj The other obj to compare against.
         * @return True, if this and {@code obj} are equal. False otherwise.
         */
        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof TierInfo) {
                final TierInfo otherInfo = (TierInfo)obj;
                return tierType() == otherInfo.tierType()
                        && isSizeFlexible() == otherInfo.isSizeFlexible()
                        && tierFamily().equals(otherInfo.tierFamily())
                        && (isSizeFlexible() || tierOid() == otherInfo.tierOid());
            } else {
                return false;
            }
        }

        @Check
        protected void validate() {

            Preconditions.checkState(!isSizeFlexible() || tierFamily().isPresent(),
                    "Size flexible RIs must have a tier family");

            Preconditions.checkState(TIER_ENTITY_TYPES.contains(tierType()),
                    "tierType() must be a valid tier entity type");
        }

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link TierInfo}.
         */
        public static class Builder extends ImmutableTierInfo.Builder {}
    }
}
