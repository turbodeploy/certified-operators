package com.vmturbo.auth.api.authentication.credentials;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.authentication.ICredentials;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;

/**
 * The JWTAuthenticationToken implements the authentication token.
 */
public class JWTAuthenticationToken extends AbstractAuthenticationToken {
    /**
     * The credentials.
     */
    private final JWTAuthorizationToken token_;

    /**
     * Constructs the JWT authentication token.
     *
     * @param token The authorization/authentication token.
     */
    public JWTAuthenticationToken(final @Nonnull JWTAuthorizationToken token) {
        super(null);
        token_ = token;
    }

    /**
     * Constructs the JWT authentication token.
     *
     * @param token The authorization/authentication token.
     * @param authorities The list of authorities.
     */
    public JWTAuthenticationToken(final @Nonnull JWTAuthorizationToken token,
            final @Nonnull Collection<? extends GrantedAuthority> authorities) {
        super(authorities);
        token_ = token;
    }

    /**
     * Returns the credentials.
     *
     * @return The credentials.
     */
    @Override public Object getCredentials() {
        return token_;
    }

    /**
     * Returns the principal.
     *
     * @return The principal.
     */
    @Override public Object getPrincipal() {
        return null;
    }

}
