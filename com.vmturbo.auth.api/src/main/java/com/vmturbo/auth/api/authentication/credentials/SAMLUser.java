package com.vmturbo.auth.api.authentication.credentials;

import java.util.Collection;

import javax.annotation.Nonnull;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * Models core SAML user information retrieved by a {@link UserDetailsService}.
 * This implementation also stores the UserApiDTO object {@code UserApiDTO}, which
 * will be retrieved by UI later, e.g. {@code SessionUtil}.
 * <p>
 * <p>
 * Note that this implementation is not immutable. It implements the
 * {@code CredentialsContainer} interface, in order to allow the password to be erased
 * after authentication. This may cause side-effects if you are storing instances
 * in-memory and reusing them. If so, make sure you return a copy from your
 * {@code UserDetailsService} each time it is invoked.
 */
final public class SAMLUser extends User {

    private final String remoteClientIP;
    private AuthUserDTO authUserDTO;

    public SAMLUser(@Nonnull final String username,
                    @Nonnull final String password,
                    @Nonnull final Collection<? extends GrantedAuthority> authorities,
                    @Nonnull final AuthUserDTO userApiDTO,
                    @Nonnull final String remoteClientIP) {
        super(username, password, authorities);
        this.authUserDTO = userApiDTO;
        this.remoteClientIP = remoteClientIP;
    }

    public AuthUserDTO getAuthUserDTO() {
        return authUserDTO;
    }

    public String getRemoteClientIP() {
        return remoteClientIP;
    }
}
