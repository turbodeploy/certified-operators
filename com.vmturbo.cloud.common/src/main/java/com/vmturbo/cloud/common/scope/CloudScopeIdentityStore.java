package com.vmturbo.cloud.common.scope;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.cloud.common.scope.CloudScopeIdentity.CloudScopeType;
import com.vmturbo.components.common.diagnostics.MultiStoreDiagnosable;

/**
 * A store for {@link CloudScopeIdentity}. Note that most classes will interact with the {@link CloudScopeIdentityProvider}
 * to assign identities to cloud scopes and not this class. This store will be used by the provider to persist assigned
 * cloud scope identities.
 */
public interface CloudScopeIdentityStore extends MultiStoreDiagnosable {

    /**
     * Saves the provided {@link CloudScopeIdentity} list.
     * @param scopeIdentityList The cloud scope identity list.
     */
    void saveScopeIdentities(@Nonnull List<CloudScopeIdentity> scopeIdentityList);

    /**
     * Gets the stored {@link CloudScopeIdentity} instances passing the provided {@link CloudScopeIdentityFilter}.
     * @param scopeIdentityFilter The cloud scope identity filter.
     * @return A list of stored cloud scope identities matching the provided filter.
     */
    @Nonnull
    List<CloudScopeIdentity> getIdentitiesByFilter(@Nonnull CloudScopeIdentityFilter scopeIdentityFilter);

    /**
     * Set the exportCloudCostDiags feature flag.
     * @param exportCloudCostDiags The feature flag status.
     */
    @Nonnull
    void setExportCloudCostDiags(boolean exportCloudCostDiags);

    /**
     * Gets the exportCloudCostDiags feature flag.
     * @return The feature flag status.
     */
    @Nonnull
    boolean getExportCloudCostDiags();

    /**
     * A filter of {@link CloudScopeIdentity} instances.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface CloudScopeIdentityFilter {

        /**
         * An all-inclusive filter.
         */
        CloudScopeIdentityFilter ALL_SCOPE_IDENTITIES = CloudScopeIdentityFilter.builder().build();

        /**
         * The set of scope IDs. Empty indicates no filtering by scope ID.
         * @return The set of scope IDs. Empty indicates no filtering by scope ID.
         */
        @Nonnull
        Set<Long> scopeIds();

        /**
         * The set of scope types. Empty indicates no filtering by scope type.
         * @return The set of scope type. Empty indicates no filtering by scope type.
         */
        @Nonnull
        Set<CloudScopeType> scopeTypes();

        /**
         * The set of account IDs. Empty indicates no filtering by account ID.
         * @return The set of account IDs. Empty indicates no filtering by account ID.
         */
        @Nonnull
        Set<Long> accountIds();

        /**
         * The set of region IDs. Empty indicates no filtering by region ID.
         * @return The set of region IDs. Empty indicates no filtering by region ID.
         */
        @Nonnull
        Set<Long> regionIds();

        /**
         * The set of resource group IDs. Empty indicates no filtering by resource group ID.
         * @return The set of resource group IDs. Empty indicates no filtering by resource group ID.
         */
        @Nonnull
        Set<Long> resourceGroupIds();

        /**
         * The set of cloud service IDs. Empty indicates no filtering by cloud service ID.
         * @return The set of cloud service IDs. Empty indicates no filtering by cloud service ID.
         */
        @Nonnull
        Set<Long> cloudServiceIds();

        /**
         * The set of service provider IDs. Empty indicates no filtering by service provider ID.
         * @return The set of service provider IDs. Empty indicates no filtering by service provider ID.
         */
        @Nonnull
        Set<Long> serviceProviderIds();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing immutable {@link CloudScopeIdentity} instances.
         */
        class Builder extends ImmutableCloudScopeIdentityFilter.Builder {}
    }

    /**
     * Retry policy for persistence operations through {@link CloudScopeIdentityStore#saveScopeIdentities(List)}.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface PersistenceRetryPolicy {

        /**
         * The default retry policy.
         */
        PersistenceRetryPolicy DEFAULT_POLICY = PersistenceRetryPolicy.builder().build();

        /**
         * The max number of retries (not attempts).
         * @return The max number of retries (not attempts).
         */
        @Default
        default int maxRetries() {
            return 3;
        }

        /**
         * The min retry delay duration. Paired with {@link #maxRetryDelay()}, this introduces jitter
         * into the backoff duration.
         * @return The min retry delay duration.
         */
        @Default
        @Nonnull
        default Duration minRetryDelay() {
            return Duration.ofSeconds(1);
        }

        /**
         * The max retry delay duration. Paired with {@link #minRetryDelay()}, this introduces jitter
         * into the backoff duration.
         * @return The max retry delay duration.
         */
        @Default
        @Nonnull
        default Duration maxRetryDelay() {
            return Duration.ofSeconds(30);
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
         * A builder class for constructing immutable {@link PersistenceRetryPolicy} instances.
         */
        class Builder extends ImmutablePersistenceRetryPolicy.Builder {}
    }
}
