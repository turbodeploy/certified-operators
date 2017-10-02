package com.vmturbo.auth.api.authorization;

import java.io.Serializable;
import javax.annotation.Nonnull;
import javax.naming.AuthenticationException;

/**
 * The IAuthorizationToken represents the opaque authentication token.
 */
public interface IAuthorizationToken extends Serializable {
    /**
     * Returns the compact representation.
     * It is package visible, since the internal representation should not be exposed anywhere else.
     *
     * @return The compact token representation.
     */
    @Nonnull String getCompactRepresentation();
}
