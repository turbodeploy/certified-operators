package com.vmturbo.cloud.common.scope;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * An identity for a {@link CloudScope} instance, including an assigned cloud scope ID.
 */
@HiddenImmutableImplementation
@Immutable
public interface CloudScopeIdentity extends CloudScope {

    /**
     * The scope ID for this cloud scope.
     * @return The scope ID for this cloud scope.
     */
    long scopeId();

    /**
     * The scope type.
     * @return The scope type.
     */
    @Nonnull
    CloudScopeType scopeType();

    /**
     * Checks whether {@link #scopeType()} is {@link CloudScopeType#AGGREGATE}.
     * @return True if {@link #scopeType()} is {@link CloudScopeType#AGGREGATE}.
     */
    default boolean isAggregateScope() {
        return scopeType() == CloudScopeType.AGGREGATE;
    }

    /**
     * Converts this identity instance to the contained {@link CloudScope}.
     * @return The {@link CloudScope} contained within this identity.
     */
    @Lazy
    default CloudScope toCloudScope() {
        return CloudScope.builder()
                .resourceId(resourceId())
                .resourceType(resourceType())
                .accountId(accountId())
                .regionId(regionId())
                .zoneId(zoneId())
                .cloudServiceId(cloudServiceId())
                .resourceGroupId(resourceGroupId())
                .serviceProviderId(serviceProviderId())
                .build();
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
     * The cloud scope type.
     */
    enum CloudScopeType {

        /**
         * A scope type pointing to a specific resource (ex: VM, DB, etc.).
         */
        RESOURCE,
        /**
         * A scope type pointing to an aggregate cloud scope (ex: account, region)
         * w.r.t. data referencing cloud scopes.
         */
        AGGREGATE
    }

    /**
     * A builder class for constructing immutable {@link CloudScopeIdentity} instances.
     */
    class Builder extends ImmutableCloudScopeIdentity.Builder {

        /**
         * Populates this builder from the provided {@link CloudScope}.
         * @param cloudScope The cloud scope.
         * @return This builder instance for method chaining.
         */
        @Nonnull
        public Builder populateFromCloudScope(@Nonnull CloudScope cloudScope) {

            return this.regionId(cloudScope.resourceId())
                    .resourceType(cloudScope.resourceType())
                    .accountId(cloudScope.accountId())
                    .regionId(cloudScope.regionId())
                    .resourceGroupId(cloudScope.resourceGroupId())
                    .cloudServiceId(cloudScope.cloudServiceId())
                    .serviceProviderId(cloudScope.serviceProviderId());
        }
    }
}
