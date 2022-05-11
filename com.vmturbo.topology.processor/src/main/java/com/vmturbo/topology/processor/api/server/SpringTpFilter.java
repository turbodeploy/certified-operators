package com.vmturbo.topology.processor.api.server;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.AUTH_HEADER_NAME;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import com.google.common.io.BaseEncoding;

import io.jsonwebtoken.SignatureException;

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
import com.vmturbo.auth.api.servicemgmt.AuthServiceHelper.ROLE;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.common.api.utils.EnvironmentUtils;
import com.vmturbo.components.common.BaseVmtComponent;

/**
 * The SpringTpFilter to intercept requests to tp in order to secure probe communication.
 */
public class SpringTpFilter extends GenericFilterBean {
    private static final String CREDENTIALS = "***";

    /**
     * The verifier.
     */
    private final JWTAuthorizationVerifier verifier_;

    private String instance;

    /**
     * Constructs the Tp Component stateless authentication filter.
     *
     * @param verifier The verifier.
     */
    public SpringTpFilter(final @Nonnull JWTAuthorizationVerifier verifier) {
        verifier_ = verifier;
        instance = EnvironmentUtils.getOptionalEnvProperty(BaseVmtComponent.PROP_INSTANCE_ID).get();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest)request;
        final String tokenAttribute = httpRequest.getHeader(AUTH_HEADER_NAME);
        if (tokenAttribute != null) { // found the JWT token
            String jwttoken = new String(BaseEncoding.base64().decode(tokenAttribute), StandardCharsets.UTF_8);
            JWTAuthorizationToken token = new JWTAuthorizationToken(jwttoken);
            try {
                AuthUserDTO dto = verifier_.verifyAuthComponent(token);
                if (PROVIDER.LDAP.equals(dto.getProvider())
                        && !dto.getRoles().contains(ROLE.PROBE_ADMIN.toString())) {
                    AuditLogUtils.logSecurityAudit(AuditAction.AUTHORIZE_PROBE,
                        instance + ": TopologyProcessor could not authorize Probe with token due to "
                        + "PROBE_ADMIN role is missing.", false);
                    throw new SecurityException("PROBE_ADMIN role is missing: " + dto.getRoles());
                }
                setSecurityContext(request, response, filterChain, dto);
            } catch (AuthorizationException | SignatureException e) {
                throw new SecurityException("Cannot authorize request with token to tp.");
            }
        } else {
            if ("external".equals(request.getParameter("source"))) {
                AuditLogUtils.logSecurityAudit(AuditAction.AUTHENTICATE_PROBE,
                    instance +  ": TopologyProcessor could not authenticate external service without token", false);
                throw new SecurityException("External probe connections require token!");
            }
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
}
