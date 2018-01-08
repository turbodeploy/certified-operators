package com.vmturbo.auth.component.store;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.authorization.AuthorizationException;

/**
 * Implements license secure storage.
 */
public interface ILicenseStore {

    /**
     * Retrieves the license.
     * The information may be retrieved by anyone.
     *
     * @return The license.
     */
    @Nonnull
    Optional<String> getLicense();

    /**
     * Store the license to file.
     *
     * @param license The license in plain text.
     * @throws AuthorizationException In case of user doesn't have Administrator role.
     */
    void populateLicense(final @Nonnull String license) throws AuthorizationException;
}
