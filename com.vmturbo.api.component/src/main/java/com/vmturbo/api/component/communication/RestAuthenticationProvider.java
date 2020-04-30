package com.vmturbo.api.component.communication;

import static com.vmturbo.api.component.external.api.service.UsersService.HTTP_ACCEPT;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
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
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.authorization.kvstore.IComponentJwtStore;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.auth.api.usermgmt.AuthorizeUserInputDTO;

public class RestAuthenticationProvider implements AuthenticationProvider {
    /**
     * The AUTH HTTP header
     */
    public static final String AUTH_HEADER_NAME = "x-auth-token";
    public static final String USERS_AUTHORIZE = "/users/authorize/";
    private static final String DUMMY_PASS = "DUMMY_PASS";

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

    private final String authRoute;

    /**
     * The REST template.
     */
    private final RestTemplate restTemplate_;

    /**
     * The verifier.
     */
    protected final JWTAuthorizationVerifier verifier_;

    /**
     * Construct the authentication provider.
     *
     * @param authHost     The authentication host.
     * @param authPort     The authentication port.
     * @param authRoute    The route prefix to use for all auth URIs.
     * @param verifier     The verifier.
     * @param restTemplate The REST endpoint.
     */
    public RestAuthenticationProvider(final @Nonnull String authHost,
                                      final int authPort,
                                      final @Nonnull String authRoute,
                                      final @Nonnull RestTemplate restTemplate,
                                      final @Nonnull JWTAuthorizationVerifier verifier) {
        authHost_ = authHost;
        authPort_ = authPort;
        this.authRoute = authRoute;
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
                                   .path(authRoute + AUTH_PATH_PREFIX);
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
        final String authRequest = newUriBuilder().build().toUriString();
        ResponseEntity<String> result;
        try {
            final AuthUserDTO authUserDTO = new AuthUserDTO(null,
                    userName, password, remoteIpAdress, null, null, Collections.EMPTY_LIST, null);
            final HttpHeaders headers = new HttpHeaders();
            headers.setAccept(HTTP_ACCEPT);
            headers.setContentType(MediaType.APPLICATION_JSON);

            final HttpEntity<AuthUserDTO> entity = new HttpEntity<>(authUserDTO, headers);
            result = restTemplate_.exchange(authRequest,
                    HttpMethod.POST, entity, String.class);
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
    @Nonnull
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

        //First try to authenticate AD users using Spring
        JWTAuthorizationToken token = getAuthData(username, password, remoteIpAddress);
        return getAuthentication(password, username, token, remoteIpAddress);
    }

    @Nonnull
    protected Authentication getAuthentication(final String password,
                                             final String username,
                                             final JWTAuthorizationToken token,
                                             final String remoteIpAddress) {
        AuthUserDTO dto;
        try {
            dto = verifier_.verify(token, Collections.emptyList());
        } catch (AuthorizationException e) {
            throw new BadCredentialsException(e.getMessage(), e);
        }
        // Initialize authorities
        final Set<GrantedAuthority> grantedAuths = new HashSet<>();
        // Process the data.
        List<String> roles = new ArrayList<String>();
        for (String role : dto.getRoles()) {
            roles.add(role);
            grantedAuths.add(new SimpleGrantedAuthority("ROLE" + "_" + role.toUpperCase()));
        }
        // also set the correct provider, use LOCAL if it's not initialized
        final PROVIDER provider = dto.getProvider() != null ? dto.getProvider() : PROVIDER.LOCAL;
        AuthUserDTO user = new AuthUserDTO(provider, username, null, remoteIpAddress, dto.getUuid(),
                                           token.getCompactRepresentation(), roles, dto.getScopeGroups());
        AuditLog.newEntry(AuditAction.LOGIN,
                "User logged in successfully", true)
                .remoteClientIP(remoteIpAddress)
                .targetName("AUTHORIZATION_SERVICE")
                .actionInitiator(username)
                .audit();
        return new UsernamePasswordAuthenticationToken(user, password, grantedAuths);
    }

    /**
     * Retrieve SAML user role from AUTH component.
     * Todo: attached unique JWT token signed by API component private key
     * @param username  SAML user name
     * @param groupName SAML user group
     * @param remoteIpAddress SAML user requested IP address
     * @param componentJwtStore API component JWT store
     * @return authentication if valid
     * @throws AuthenticationException when the roles are not found for the user
     */
    @Nonnull
    public Authentication authorize(@Nonnull final String username,
                                    @Nonnull final Optional<String> groupName,
                                    @Nonnull final String remoteIpAddress,
                                    @Nonnull final IComponentJwtStore componentJwtStore)
            throws AuthenticationException {
        final UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
                .scheme("http")
                .host(authHost_)
                .port(authPort_)
                .path(USERS_AUTHORIZE);

        final HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(RestAuthenticationProvider.AUTH_HEADER_NAME,
                componentJwtStore.generateToken().getCompactRepresentation());
        headers.set(SecurityConstant.COMPONENT_ATTRIBUTE, componentJwtStore.getNamespace());

        final HttpEntity<AuthorizeUserInputDTO> entity
         = new HttpEntity<>(new AuthorizeUserInputDTO(username, groupName.orElse(null), remoteIpAddress), headers);

        final ResponseEntity<String> result = restTemplate_.exchange(builder.toUriString(),
                HttpMethod.POST, entity, String.class);
        final JWTAuthorizationToken token = new JWTAuthorizationToken(result.getBody());
        // add a dummy password.
        return getAuthentication(DUMMY_PASS, username, token, remoteIpAddress);
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
