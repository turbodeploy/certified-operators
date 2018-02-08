package com.vmturbo.api.component.communication;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;

public class RestAuthenticationProvider implements AuthenticationProvider {
    /**
     * The AUTH HTTP header
     */
    public static final String AUTH_HEADER_NAME = "x-auth-token";

    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger(RestAuthenticationProvider.class);

    /**
     * The prefix to paths in the repository.
     */
    private static final String AUTH_PATH_PREFIX = "/users/authenticate/";

    /**
     * The auth service host.
     */
    private final String authHost_;

    /**
     * The auth service port.
     */
    private final int authPort_;

    /**
     * The REST template.
     */
    private final RestTemplate restTemplate_;

    /**
     * The verifier.
     */
    private final JWTAuthorizationVerifier verifier_;

    /**
     * Construct the authentication provider.
     *
     * @param authHost     The authentication host.
     * @param authPort     The authentication port.
     * @param verifier     The verifier.
     * @param restTemplate The REST endpoint.
     */
    public RestAuthenticationProvider(final @Nonnull String authHost,
                                      final int authPort,
                                      final @Nonnull RestTemplate restTemplate,
                                      final @Nonnull JWTAuthorizationVerifier verifier) {
        authHost_ = authHost;
        authPort_ = authPort;
        restTemplate_ = restTemplate;
        verifier_ = verifier;
    }

    /**
     * Builds the base target URI.
     *
     * @return The base target URI.
     */
    @Nonnull
    private UriComponentsBuilder newUriBuilder() {
        return UriComponentsBuilder.newInstance()
                                   .scheme("http")
                                   .host(authHost_)
                                   .port(authPort_)
                                   .path(AUTH_PATH_PREFIX);
    }

    /**
     * Returns the authentication data.
     * Note: changed the annotation of password parameter from @Nullable to @Nonnull. Since auth
     * component has @Nonnull annotation to password parameter.
     * See com.vmturbo.auth.component.store.AuthProvider#authenticate()
     *
     * @param userName The user name.
     * @param password The password.
     * @param remoteIpAdress the user's IP address.
     * @return The authorization token if the userName and password are valid.
     * @throws AuthenticationException In case authentication has failed.
     */
    private JWTAuthorizationToken getAuthData(final @Nonnull String userName,
                                              final @Nonnull String password,
                                              final @Nonnull String remoteIpAdress)
            throws AuthenticationException {
        final String authRequest = newUriBuilder()
                .pathSegment(
                        encodeValue(userName),
                        encodeValue(password),
                        remoteIpAdress)
                .build()
                .toUriString();
        ResponseEntity<String> result;
        try {
            result = restTemplate_.getForEntity(authRequest, String.class);
            return new JWTAuthorizationToken(result.getBody());
        } catch (HttpServerErrorException e) {
            throw new BadCredentialsException("Error authenticating user " + userName, e);
        }
    }

    /**
     * Performs authentication with the request object passed in. It will first try to authenticate
     * through Active Directory which is setup in UI. And then it will try to authenticate through
     * local users which are defined in "login.config.topology". If request is authenticated, it
     * will
     * grant an authority to authentication object. The authority is currently the role of user and
     * will be used to verify if a resource can be accessed by a user.
     *
     * @param authentication the authentication request object.
     * @return a fully authenticated object including credentials and authorities. May return
     * <code>null</code> if the <code>AuthenticationProvider</code> is unable to support
     * authentication of the passed <code>Authentication</code> object. In such a case,
     * the next <code>AuthenticationProvider</code> that supports the presented
     * <code>Authentication</code> class will be tried.
     * @throws AuthenticationException if authentication fails.
     */
    @Override
    public Authentication authenticate(Authentication authentication)
            throws AuthenticationException {
        Object principal = authentication.getPrincipal();
        String password = authentication.getCredentials().toString();
        String remoteIpAddress = authentication.getDetails().toString();

        String username;
        if (principal instanceof UserApiDTO) {
            username = ((UserApiDTO)principal).getUsername();
        } else {
            username = authentication.getName();
        }

        // Initialize authorities
        final Set<GrantedAuthority> grantedAuths = new HashSet<>();
        //First try to authenticate AD users using Spring
        JWTAuthorizationToken token = getAuthData(username, password, remoteIpAddress);
        AuthUserDTO dto;
        try {
            dto = verifier_.verify(token, Collections.emptyList());
        } catch (AuthorizationException e) {
            throw new BadCredentialsException(e.getMessage());
        }
        // Process the data.
        List<String> roles = new ArrayList<String>();
        for (String role : dto.getRoles()) {
            roles.add(role);
            grantedAuths.add(new SimpleGrantedAuthority("ROLE" + "_" + role.toUpperCase()));
        }
        AuthUserDTO user = new AuthUserDTO(PROVIDER.LOCAL, username, null, dto.getUuid(),
                                           token.getCompactRepresentation(), roles);
        return new UsernamePasswordAuthenticationToken(user, password, grantedAuths);
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authentication.equals(UsernamePasswordAuthenticationToken.class);
    }

    /**
     * Encode value based on UTF_8.
     *
     * @param value original string
     * @return encoded value
     */
    private String encodeValue(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage());
            return value; // will try the original value.
        }
    }
}
