package com.vmturbo.auth.component.spring;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.AUTH_HEADER_NAME;

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

import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.servicemgmt.AuthServiceDTO;
import com.vmturbo.auth.api.servicemgmt.AuthServiceHelper.ROLE;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.api.utils.EnvironmentUtils;
import com.vmturbo.components.common.BaseVmtComponent;

/**
 * The SpringAuthFilter implements the stateless authentication filter.
 */
public class SpringAuthFilter extends GenericFilterBean {
    private static final String CREDENTIALS = "***";

    /**
     * The verifier
     */
    private final JWTAuthorizationVerifier verifier_;

    private String instance;

    /**
     * Constructs the AUTH Component stateless authentication filter.
     *
     * @param verifier The verifier.
     */
    public SpringAuthFilter(final @Nonnull JWTAuthorizationVerifier verifier) {
        verifier_ = verifier;
        instance = EnvironmentUtils.getOptionalEnvProperty(BaseVmtComponent.PROP_INSTANCE_ID).get();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        final String tokenAttribute = httpRequest.getHeader(AUTH_HEADER_NAME);
        final String componentAttribute = httpRequest.getHeader(SecurityConstant.COMPONENT_ATTRIBUTE);
        final String oauth2Attribute = httpRequest.getHeader(SecurityConstant.OAUTH2_HEADER_NAME);
        if (tokenAttribute != null) { // found the JWT token
            JWTAuthorizationToken token = new JWTAuthorizationToken(tokenAttribute);
            try {
                if (oauth2Attribute != null && oauth2Attribute.equals(SecurityConstant.HYDRA)) {
                    setSecurityContext(request, response, filterChain, verifier_.verifyHydraToken(token));
                } else {
                    AuthUserDTO dto;
                    if (componentAttribute != null) { // component request, verify with component public key
                        dto = verifier_.verifyComponent(token, componentAttribute);
                    } else { // user request, verify with Auth private key
                        dto = verifier_.verify(token, Collections.emptyList());
                    }
                    setSecurityContext(request, response, filterChain, dto);
                }
            } catch (AuthorizationException e) {
                AuditLogUtils.logSecurityAudit(AuditAction.AUTHENTICATE_PROBE,
                    instance + ": Verifier could not authenticate token.", false);
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
        AuditLogUtils.logSecurityAudit(AuditAction.SET_SECURITY_CONTEXT,
            instance + ": Successfully set security context for service: " + dto.getUser(), true);
        filterChain.doFilter(request, response);
    }

    private void setSecurityContext(@Nonnull final ServletRequest request,
            @Nonnull final ServletResponse response,
            @Nonnull final FilterChain filterChain,
            @Nonnull final AuthServiceDTO dto) throws IOException, ServletException {
        Objects.requireNonNull(request);
        Objects.requireNonNull(response);
        Objects.requireNonNull(filterChain);
        Objects.requireNonNull(dto);
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        for (ROLE role : dto.getRoles()) {
            grantedAuths.add(new SimpleGrantedAuthority(SecurityConstant.ROLE_STRING + role));
        }
        Authentication authentication = new UsernamePasswordAuthenticationToken(dto,
        CREDENTIALS,
        grantedAuths);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        AuditLogUtils.logSecurityAudit(AuditAction.SET_SECURITY_CONTEXT,
            instance + ": Successfully set security context for service: " + dto.getName(), true);
        filterChain.doFilter(request, response);
    }
}
