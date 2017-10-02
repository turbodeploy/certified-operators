package com.vmturbo.auth.api.authorization.jwt;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.authorization.IAuthorizationToken;

/**
 * The JWTAuthorizationToken implements the JWT authentication token.
 */
public class JWTAuthorizationToken implements IAuthorizationToken {
    /**
     * The compact JWT representation.
     */
    private final String compactRepresentation_;

    /**
     * Constructs the JWT Authentication Token.
     *
     * @param compactRepresentation The compact token representation.
     */
    public JWTAuthorizationToken(final @Nonnull String compactRepresentation) {
        String tmp = compactRepresentation;
        if (compactRepresentation.startsWith("\"")) {
            tmp = tmp.substring(1, tmp.length() - 1);
        }
        compactRepresentation_ = tmp;
    }

    /**
     * Checks whether this object equals the one passed in.
     *
     * @param o The object to test against.
     * @return {@code true} iff this equals to {@code o}
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof JWTAuthorizationToken)) {
            return false;
        }
        return compactRepresentation_.equals(((JWTAuthorizationToken)o).compactRepresentation_);
    }

    /**
     * Returns the hash code of the JWT authentication token.
     *
     * @return The hash code of the JWT authentication token.
     */
    @Override
    public int hashCode() {
        return compactRepresentation_.hashCode();
    }

    /**
     * Returns the compact representation.
     * It is package visible, since the internal representation should not be exposed anywhere else.
     *
     * @return The compact token representation.
     */
    public @Nonnull String getCompactRepresentation() {
        return compactRepresentation_;
    }
}
