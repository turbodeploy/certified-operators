package com.vmturbo.cloud.common.scope;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Immutable data class containing the immutable attributes of an entity constituting possible
 * cloud scopes.
 */
@HiddenImmutableImplementation
@Immutable(prehash = true)
public interface CloudScope {

    /**
     * The resource ID. May be null, if this is an aggregate cloud scope (e.g account or region).
     * @return The resource ID.
     */
    @Nullable
    Long resourceId();

    /**
     * The resource type. Should be set whenever {@link #resourceId()} is set.
     * @return The resource type.
     */
    @Nullable
    EntityType resourceType();

    /**
     * Checks whether both {@link #resourceId()} and {@link #resourceType()} are set.
     * @return True if both {@link #resourceId()} and {@link #resourceType()} are set.
     */
    default boolean hasResourceInfo() {
        return resourceId() != null && resourceType() != null;
    }

    /**
     * The account ID.
     * @return The account ID.
     */
    @Nullable
    Long accountId();

    /**
     * The region ID.
     * @return The region ID.
     */
    @Nullable
    Long regionId();

    /**
     * The availability zone ID.
     * @return The availability zone ID.
     */
    @Nullable
    Long zoneId();

    /**
     * The cloud service ID.
     * @return The cloud service ID.
     */
    @Nullable
    Long cloudServiceId();

    /**
     * The resource group ID.
     * @return The resource group ID.
     */
    @Nullable
    Long resourceGroupId();

    /**
     * The service provider ID.
     * @return The service provider ID.
     */
    long serviceProviderId();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutable {@link CloudScope} instances.
     */
    class Builder extends ImmutableCloudScope.Builder {}
}
