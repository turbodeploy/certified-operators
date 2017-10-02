package com.vmturbo.api.component.external.api.service;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.api.component.external.api.mapper.LoginProviderMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.ErrorApiDTO;
import com.vmturbo.api.dto.UserApiDTO;
import com.vmturbo.api.serviceinterfaces.IAuthenticationService;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
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
    @Autowired
    private final JWTAuthorizationVerifier verifier_;

    /**
     * Constructs the authentication service.
     *
     * @param authHost     The authentication host.
     * @param authPort     The authentication port.
     * @param verifier     The verifier.
     * @param restTemplate The REST endpoint.
     */
    public AuthenticationService(final @Nonnull String authHost,
                                 final int authPort,
                                 final @Nonnull JWTAuthorizationVerifier verifier,
                                 final @Nonnull RestTemplate restTemplate) {
        authHost_ = authHost;
        authPort_ = authPort;
        verifier_ = verifier;
        restTemplate_ = restTemplate;
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
        result = restTemplate_.getForEntity(request, Boolean.class);
        return result.getBody().booleanValue();
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
        restTemplate_.postForObject(builder.build().toUriString(), dto, String.class);
        UserApiDTO user = new UserApiDTO();
        user.setUsername(username);
        return user;
    }

    @Override
    public BaseApiDTO login(String username, String password, Boolean remember) {

        // TODO (roman, May 24 2017): Determine what the proper handling of "remember" should be.

        RestAuthenticationProvider authProvider = new RestAuthenticationProvider(
                authHost_,
                authPort_,
                restTemplate_,
                verifier_);
        UserApiDTO user = new UserApiDTO();
        user.setUsername(username);
        Authentication auth = new UsernamePasswordAuthenticationToken(user, password);

        // authenticate
        try {
            Authentication result = authProvider.authenticate(auth);
            if (result != null) {
                // prevent session fixation attack, it should be put before setting security context.
                changeSessionId();
                SecurityContextHolder.getContext().setAuthentication(result);
                AuthUserDTO dto = (AuthUserDTO)result.getPrincipal();
                user.setUuid(dto.getUuid());
                user.setLoginProvider(LoginProviderMapper.toApi(dto.getProvider()));
                user.setAuthToken(dto.getToken());
                return user;
            }
        } catch (AuthenticationException e) {
            ErrorApiDTO error = new ErrorApiDTO();
            error.setMessage("Authentication Failed");
            return error;
        }

        ErrorApiDTO error = new ErrorApiDTO();
        error.setMessage("FAIL");
        return error;
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
}
