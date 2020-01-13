package com.vmturbo.auth.component.spring;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;

import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * The SpringAuthFilter implements the stateless authentication filter.
 */
public class SpringAuthFilter extends GenericFilterBean {
    /**
     * The AUTH HTTP header
     */
    private static final String AUTH_HEADER_NAME = "x-auth-token";
    private static final String CREDENTIALS = "***";

    /**
     * The verifier
     */
    private final JWTAuthorizationVerifier verifier_;

    /**
     * Constructs the AUTH Component stateless authentication filter.
     *
     * @param verifier The verifier.
     */
    public SpringAuthFilter(final @Nonnull JWTAuthorizationVerifier verifier) {
        verifier_ = verifier;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        final String tokenAttribute = httpRequest.getHeader(AUTH_HEADER_NAME);
        final String componentAttribute = httpRequest.getHeader(SecurityConstant.COMPONENT_ATTRIBUTE);

        if (tokenAttribute != null) { // found the JWT token
            JWTAuthorizationToken token = new JWTAuthorizationToken(tokenAttribute);
            try {
                AuthUserDTO dto;
                if (componentAttribute != null) { // component request, verify with component public key
                    dto = verifier_.verifyComponent(token, componentAttribute);
                } else { // user request, verify with Auth private key
                    dto = verifier_.verify(token, Collections.emptyList());
                }
                setSecurityContext(request, response, filterChain, dto);
            } catch (AuthorizationException e) {
                throw new SecurityException(e);
            }
        } else {
            filterChain.doFilter(request, response);
        }
    }

    private void setSecurityContext(@Nonnull final ServletRequest request,
                                    @Nonnull final ServletResponse response,
                                    @Nonnull final FilterChain filterChain,
                                    @Nonnull final AuthUserDTO dto) throws IOException, ServletException {
        Objects.requireNonNull(request);
        Objects.requireNonNull(response);
        Objects.requireNonNull(filterChain);
        Objects.requireNonNull(dto);
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        for (String role : dto.getRoles()) {
            grantedAuths.add(new SimpleGrantedAuthority(SecurityConstant.ROLE_STRING + role.toUpperCase()));
        }

        // The password is hidden.
        // put the whole AuthUserDTO (rather than only username) into authentication as we may need
        // to get current user's uuid or other attributes
        Authentication authentication = new UsernamePasswordAuthenticationToken(dto,
                CREDENTIALS,
                grantedAuths);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        filterChain.doFilter(request, response);
    }
}
