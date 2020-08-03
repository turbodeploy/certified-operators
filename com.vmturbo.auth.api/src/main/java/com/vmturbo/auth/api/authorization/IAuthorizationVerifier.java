package com.vmturbo.auth.api.authorization;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * An IAuthorizationVerifier has a sole function of verifying an authentication token.
 */
public interface IAuthorizationVerifier {
    /**
     * The JWT token claim for roles.
     */
    String ROLE_CLAIM = "ROLES";

    /**
     * The JWT token claim for scope.
     */
    String SCOPE_CLAIM = "SCOPE";

    /**
     * The JWT token claim for login provider.
     */
    String PROVIDER_CLAIM = "PROVIDER";

    /**
     * Verifies the validity of the token.
     *
     * @param token         The existing authentication token.
     * @param expectedRoles The collection of roles the caller claims to have.
     * @return An {@link AuthUserDTO} object describing the token's principal as well as its
     *                                authorized roles and UUID.
     * @throws AuthorizationException If verification fails.
     */
    AuthUserDTO verify(final @Nonnull IAuthorizationToken token,
                       final @Nonnull Collection<String> expectedRoles)
            throws AuthorizationException;

    AuthUserDTO verifyComponent(JWTAuthorizationToken token, String component) throws AuthorizationException;
}
