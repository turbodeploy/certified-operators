package com.vmturbo.auth.component.spring;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;

/**
 * The SpringAuthFilter implements the stateless authentication filter.
 */
public class SpringAuthFilter extends GenericFilterBean {
    /**
     * The AUTH HTTP header
     */
    private static final String AUTH_HEADER_NAME = "x-auth-token";

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
        HttpServletRequest httpRequest = (HttpServletRequest)request;
        final String tokenAttribute = httpRequest.getHeader(AUTH_HEADER_NAME);
        if (tokenAttribute != null) {
            JWTAuthorizationToken token = new JWTAuthorizationToken(tokenAttribute);
            try {
                AuthUserDTO dto = verifier_.verify(token, Collections.emptyList());
                Set<GrantedAuthority> grantedAuths = new HashSet<>();
                for (String role : dto.getRoles()) {
                    grantedAuths.add(new SimpleGrantedAuthority("ROLE" + "_" + role.toUpperCase()));
                }

                // The password is hidden.
                Authentication authentication = new UsernamePasswordAuthenticationToken(dto.getUser(),
                                                                                        "***",
                                                                                        grantedAuths);
                SecurityContextHolder.getContext().setAuthentication(authentication);
                filterChain.doFilter(request, response);
            } catch (AuthorizationException e) {
                throw new SecurityException(e);
            }
        } else {
            filterChain.doFilter(request, response);
        }
    }
}
