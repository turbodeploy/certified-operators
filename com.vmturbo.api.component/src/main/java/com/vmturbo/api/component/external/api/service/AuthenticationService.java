package com.vmturbo.api.component.external.api.service;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.api.component.external.api.mapper.LoginProviderMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.ErrorApiDTO;
import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.api.exceptions.InvalidCredentialsException;
import com.vmturbo.api.exceptions.ServiceUnavailableException;
import com.vmturbo.api.serviceinterfaces.IAuthenticationService;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.kvstore.ComponentJwtStore;
import com.vmturbo.auth.api.authorization.kvstore.IComponentJwtStore;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * Service layer to implement Authentication.
 * Note that this implementation is hard coded and only handles a single user and role.
 * Also note that the fields for users and roles are taken from the Legacy implementation, and
 * hence will likely
 * change as the system evolves.
 * <p>
 * THERE IS NO DATABASE BEHIND THIS IMPLEMENTATION - AND HENCE NO REAL SEARCH!
 * <p>
 * TODO: Replace this implementation with a real User database in a future task.
 **/

public class AuthenticationService implements IAuthenticationService {

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
     * To provide component based JWT token.
     */
    private final IComponentJwtStore componentJwtStore_;

    /**
     * The remote HTTP request.
     */
    @Autowired
    private HttpServletRequest request;

    /**
     * The error message returned if the Auth component is not running.
     */
    private static final String AUTH_SERVICE_NOT_AVAILABLE_MSG =
            "The Authorization Service is not responding";

    private final Logger logger = LogManager.getLogger(getClass());

    public AuthenticationService(final @Nonnull String authHost,
                                 final int authPort,
                                 final @Nonnull JWTAuthorizationVerifier verifier,
                                 final @Nonnull RestTemplate restTemplate,
                                 final @Nonnull IComponentJwtStore componentJwtStore) {
        authHost_ = authHost;
        authPort_ = authPort;
        verifier_ = verifier;
        restTemplate_ = restTemplate;
        componentJwtStore_ = componentJwtStore;
    }

    /**
     * Checks whether the admin has been initialized.
     *
     * @return {@code true} if the admin user has been initialized, {@code false} otherwise.
     */
    @Override
    public boolean checkInit() {
        UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
                                                           .scheme("http")
                                                           .host(authHost_)
                                                           .port(authPort_)
                                                           .path("/users/checkAdminInit");
        final String request = builder.build().toUriString();
        ResponseEntity<Boolean> result;
        try {
            result = restTemplate_.getForEntity(request, Boolean.class);
            return result.getBody().booleanValue();
        } catch (RestClientException e) {
            throw new ServiceUnavailableException(AUTH_SERVICE_NOT_AVAILABLE_MSG);
        }
    }

    /**
     * Initialize the admin user.
     *
     * @param username The user name.
     * @param password The password.
     * @return The {@code users://user_name} URL if successful.
     */
    public BaseApiDTO initAdmin(String username, String password) {
        UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
                                                           .scheme("http")
                                                           .host(authHost_)
                                                           .port(authPort_)
                                                           .path("/users/initAdmin");
        AuthUserDTO dto = new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, username, password, null,
                                          null, ImmutableList.of("ADMINISTRATOR"));
        try {
            restTemplate_.postForObject(builder.build().toUriString(), dto, String.class);
            UserApiDTO user = new UserApiDTO();
            user.setUsername(username);
            return user;
        } catch(RestClientException e) {
            throw new ServiceUnavailableException(AUTH_SERVICE_NOT_AVAILABLE_MSG);
        }
    }

    @Override
    public BaseApiDTO login(String username, String password, Boolean remember)
            throws InvalidCredentialsException {

        // TODO (roman, May 24 2017): Determine what the proper handling of "remember" should be.

        RestAuthenticationProvider authProvider = new RestAuthenticationProvider(
                authHost_,
                authPort_,
                restTemplate_,
                verifier_);
        UserApiDTO user = new UserApiDTO();
        user.setUsername(username);

        // Change to child interface AbstractAuthenticationToken to store IP address.
        AbstractAuthenticationToken auth = new UsernamePasswordAuthenticationToken(user, password);
        // If client IP is not available from the request, set the IP  to local IP address.
        String remoteIpAddress = ApiUtils.getClientIp(request).orElse(ApiUtils.getLocalIpAddress());
        //Pass IP address to authentication token details, so it can be retrieved later.
        //{@link org.springframework.security.core.Authentication#getDetails}
        auth.setDetails(remoteIpAddress);

        // authenticate
        try {
            Authentication result = authProvider.authenticate(auth);
                // prevent session fixation attack, it should be put before setting security context.
            changeSessionId();
            SecurityContextHolder.getContext().setAuthentication(result);
            AuthUserDTO dto = (AuthUserDTO)result.getPrincipal();
            user.setUuid(dto.getUuid());
            user.setLoginProvider(LoginProviderMapper.toApi(dto.getProvider()));
            user.setAuthToken(dto.getToken());
            // just like legacy, pass the user role to UI
            if (!dto.getRoles().isEmpty()) {
                user.setRoleName(dto.getRoles().get(0));
            }
            // TODO: it's a hack to avoid infinite loop, remove it when OM-24011 is fixed
            setSessionMaxInactiveInterval(0);
            return user;
        } catch (AuthenticationException e) {
            logger.warn("Authentication for user " + username + " failed", e);
            throw new InvalidCredentialsException("Authentication failed");
        } catch (RestClientException e) {
            throw new ServiceUnavailableException(AUTH_SERVICE_NOT_AVAILABLE_MSG);
        }
    }

    /**
     * Authorize SAML user.
     *
     * @param username user name
     * @param groupName group name
     * @param ipAddress user IP address
     * @return {@link AuthUserDTO}
     */
    public Optional<AuthUserDTO> authorize(String username, Optional<String> groupName, String ipAddress) {
        RestAuthenticationProvider authProvider = new RestAuthenticationProvider(
                authHost_,
                authPort_,
                restTemplate_,
                verifier_);
        Authentication result = authProvider.authorize(username, groupName, ipAddress, componentJwtStore_);
        return Optional.ofNullable((AuthUserDTO) result.getPrincipal());
    }


    @Override
    public BaseApiDTO logout() {
        final Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        final RequestAttributes attrs = RequestContextHolder.currentRequestAttributes();
        final HttpServletRequest request = ((ServletRequestAttributes)attrs).getRequest();
        final HttpServletResponse response = ((ServletRequestAttributes)attrs).getResponse();
        if (auth != null) {
            new SecurityContextLogoutHandler().logout(request, response, auth);
            BaseApiDTO success = new BaseApiDTO();
            success.setDisplayName("SUCCESS");
            return success;
        }

        ErrorApiDTO error = new ErrorApiDTO();
        error.setMessage("FAIL");
        return error;
    }

    /**
     * The HttpServletRequest.changeSessionId() is the default method for protecting against
     * Session Fixation attacks in Servlet 3.1 and higher.
     * @see <a href="https://docs.spring.io/spring-security/site/docs/current/reference/html/servletapi.html/">
     * Servlet API integration</a>
     * {@link com.vmturbo.api.component.external.api.ApiSecurityConfig#configure}
     */
    private void changeSessionId() {
        final RequestAttributes attrs = RequestContextHolder.currentRequestAttributes();
        final HttpServletRequest request = ((ServletRequestAttributes)attrs).getRequest();
        if (request != null && request.getSession(false) != null) {
            request.changeSessionId();
        }
    }

    /**
     * Specifies the time, in seconds, between client requests before the
     * servlet container will invalidate this session. A zero or negative time
     * indicates that the session should never timeout.
     *
     * @param interval An integer specifying the number of seconds
     */
    private void setSessionMaxInactiveInterval(int interval) {
        final RequestAttributes attrs = RequestContextHolder.currentRequestAttributes();
        final HttpServletRequest request = ((ServletRequestAttributes)attrs).getRequest();
        if (request != null && request.getSession() != null) {
            request.getSession().setMaxInactiveInterval(interval);
        }
    }
}
