package com.vmturbo.api.component.security;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import com.vmturbo.api.component.communication.CustomSamlAuthenticationToken;
import com.vmturbo.api.component.external.api.util.ApiUtils;

/**
 * Custom SAML filter to store client IP.
 */
public class CustomSamlAuthenticationFilter extends OncePerRequestFilter {

    private static final String UNKNOWN_IP = "Unknown IP";
    private static final String SAML2_SERVLET_PATH = "/vmturbo/saml2";

    /**
     * Constructor.
     */
    public CustomSamlAuthenticationFilter() {
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {
        SecurityContext context = SecurityContextHolder.getContext();
        final String servletPath = request.getServletPath();
        if (context != null && servletPath != null && servletPath.contains(SAML2_SERVLET_PATH)) {
            Authentication authenticationToken = context.getAuthentication();
            // If it's not authenticated, store request IP to token.
            if (authenticationToken == null) {
                final String remoteIpAddress = ApiUtils.getClientIp(request).orElse(UNKNOWN_IP);
                CustomSamlAuthenticationToken customSamlAuthenticationToken =
                        new CustomSamlAuthenticationToken(remoteIpAddress);
                SecurityContextHolder.getContext().setAuthentication(customSamlAuthenticationToken);
            }
        }
        filterChain.doFilter(request, response);
    }
}