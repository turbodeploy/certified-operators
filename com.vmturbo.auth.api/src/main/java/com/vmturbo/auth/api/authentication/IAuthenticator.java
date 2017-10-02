package com.vmturbo.auth.api.authentication;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.authorization.IAuthorizationToken;

/**
 * The IAuthenticator represents authentication API.
 */
public interface IAuthenticator {
    /**
     * Performs the authentication.
     *
     * @param credentials The credentials.
     * @return The authentication token.
     * @throws AuthenticationException In case of the authentication failure.
     */
    @Nonnull
    IAuthorizationToken authenticate(final @Nonnull ICredentials credentials)
            throws AuthenticationException;
}
