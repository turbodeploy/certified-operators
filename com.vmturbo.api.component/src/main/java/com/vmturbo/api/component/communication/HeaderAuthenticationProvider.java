package com.vmturbo.api.component.communication;

import java.security.PublicKey;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.api.component.security.HeaderMapper;
import com.vmturbo.api.component.security.IdTokenVerifier;
import com.vmturbo.auth.api.Pair;
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
    private final IdTokenVerifier idTokenVerifier;
    private final int clockSkewSecond;

    /**
     * Construct the authentication provider.
     *
     * @param authHost The authentication host.
     * @param authPort The authentication port.
     * @param authRoute
     * @param restTemplate The REST endpoint.
     * @param verifier The verifier.
     * @param componentJwtStore The component JWT store.
     * @param idTokenVerifier JWT (id) token verifier
     * @param clockSkewSecond clock skew in seconds
     */
    public HeaderAuthenticationProvider(final @Nonnull String authHost, final int authPort,
            String authRoute, final @Nonnull RestTemplate restTemplate,
            final @Nonnull JWTAuthorizationVerifier verifier,
            final @Nonnull IComponentJwtStore componentJwtStore,
            final @Nonnull IdTokenVerifier idTokenVerifier, final int clockSkewSecond) {
        super(authHost, authPort, authRoute, restTemplate, verifier);
        this.componentJwtStore = Objects.requireNonNull(componentJwtStore);
        this.idTokenVerifier = Objects.requireNonNull(idTokenVerifier);
        Preconditions.checkArgument(clockSkewSecond > 30);
        this.clockSkewSecond = clockSkewSecond;
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
        final Optional<PublicKey> publicKey = authorizationToken.getPublicKey();
        final HeaderMapper headerMapper = authorizationToken.getHeaderMapper();
        final boolean isLatest = authorizationToken.isLatest();

        try {
            if (publicKey.isPresent() && jwtToken.isPresent()) {
                final Pair<String, String> userAndRolePair = isLatest ?
                        idTokenVerifier.verifyLatest(publicKey, jwtToken, clockSkewSecond) :
                        idTokenVerifier.verify(publicKey, jwtToken, clockSkewSecond);
                final String tokenUserName = userAndRolePair.first;
                final Optional<String> tokenGroup =
                        Optional.ofNullable(headerMapper.getAuthGroup(userAndRolePair.second));
                return super.authorize(tokenUserName, tokenGroup, ipAddress, componentJwtStore);
            } else {
                return super.authorize(username, Optional.of(group), ipAddress, componentJwtStore);
            }
        } catch (com.vmturbo.auth.api.authentication.AuthenticationException e) {
            throw new BadCredentialsException("Failed to validate JWT token.");
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
