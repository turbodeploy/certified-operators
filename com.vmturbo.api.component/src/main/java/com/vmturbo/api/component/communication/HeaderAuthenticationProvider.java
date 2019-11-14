package com.vmturbo.api.component.communication;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.CREDENTIALS;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.kvstore.IComponentJwtStore;

/**
 * Custom Spring authentication provider to authenticate and authorize REST request based on
 * properties passed in from header. For detail properties support, see {@link
 * HeaderAuthenticationToken}.
 */
public class HeaderAuthenticationProvider extends RestAuthenticationProvider {

    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger(HeaderAuthenticationProvider.class);
    private final IComponentJwtStore componentJwtStore;

    /**
     * Construct the authentication provider.
     *  @param authHost The authentication host.
     * @param authPort The authentication port.
     * @param authRoute
     * @param restTemplate The REST endpoint.
     * @param verifier The verifier.
     * @param componentJwtStore The component JWT store.
     */
    public HeaderAuthenticationProvider(final @Nonnull String authHost, final int authPort,
            String authRoute, final @Nonnull RestTemplate restTemplate,
            final @Nonnull JWTAuthorizationVerifier verifier,
            final @Nonnull IComponentJwtStore componentJwtStore) {
        super(authHost, authPort, authRoute, restTemplate, verifier);
        this.componentJwtStore = Objects.requireNonNull(componentJwtStore);
    }

    /**
     * Performs authentication with the request object passed in. It will grant an authority to
     * authentication object. The authority is currently the role of user and
     * will be used to verify if a resource can be accessed by a user.
     *
     * @param authentication the authentication request object.
     * @return a fully authenticated object including credentials and authorities. May return
     *         <code>null</code> if the <code>AuthenticationProvider</code> is unable to support
     *         authentication of the passed <code>Authentication</code> object. In such a case,
     *         the next <code>AuthenticationProvider</code> that supports the presented
     *         <code>Authentication</code> class will be tried.
     * @throws AuthenticationException if authentication fails.
     */
    @Override
    @Nonnull
    public Authentication authenticate(Authentication authentication)
            throws AuthenticationException {

        // it safe to to cast here, due to only authentication instance is assignable from
        // {@link HeaderAuthenticationToken} will reach here. It's filtered by overriding the supports
        // method from base class.
        final HeaderAuthenticationToken authorizationToken =
                (HeaderAuthenticationToken)authentication;
        final String username = authorizationToken.getUserName();
        final String group = authorizationToken.getGroup();
        final String ipAddress = authorizationToken.getRemoteIpAddress();
        final Optional<String> jwtToken = authorizationToken.getJwtToken();
        try {
            return jwtToken.map(jwt -> super.getAuthentication(CREDENTIALS, username,
                    new JWTAuthorizationToken(jwt), ipAddress))
                    .orElseGet(() -> super.authorize(username, Optional.of(group), ipAddress,
                            componentJwtStore));
        } catch (HttpServerErrorException e) {
            logger.error("Failed to authenticate request with user: {}", username);
            throw new BadCredentialsException("Authentication failed");
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return HeaderAuthenticationToken.class.isAssignableFrom(authentication);
    }
}
