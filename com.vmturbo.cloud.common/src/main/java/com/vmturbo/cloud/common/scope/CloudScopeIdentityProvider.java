package com.vmturbo.cloud.common.scope;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.RequiresDataInitialization;

/**
 * A provider of {@link CloudScopeIdentity} (including the cloud scope ID), based on provided scoping
 * attributes, contained within individual {@link CloudScope} instances.
 */
public interface CloudScopeIdentityProvider extends RequiresDataInitialization {

    /**
     * Either fetches previously assigned cloud scope identities or creates new identities (if a prior
     * identity is not found) for the provided list of {@link CloudScope} instances.
     * @param cloudScopes The cloud scope instances.
     * @return An immutable list of cloud scope identities, in which the order of identities within the
     * list mirrors that of the {@code cloudScopes} input parameter.
     * @throws IdentityOperationException Thrown if there is an exception in storing the cloud scope
     * identities.
     * @throws IdentityUninitializedException Thrown if this method is invoked prior to underlying cache
     * data being fully initialized.
     */
    @Nonnull
    Map<CloudScope, CloudScopeIdentity> getOrCreateScopeIdentities(@Nonnull Collection<CloudScope> cloudScopes)
            throws IdentityOperationException, IdentityUninitializedException;
}
