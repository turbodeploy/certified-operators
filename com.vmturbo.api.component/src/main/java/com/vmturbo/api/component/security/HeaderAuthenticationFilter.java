package com.vmturbo.api.component.security;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.X_TURBO_ROLE;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.X_TURBO_TOKEN;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import com.vmturbo.api.component.communication.HeaderAuthenticationToken;
import com.vmturbo.api.component.external.api.util.ApiUtils;

/**
 * <P>
 * Custom Spring filter to parse request headers.
 * </P>
 * Current required properties, either:
 * <ul>
 * <li>x-barracuda-account - user name
 * <li>x-barracuda-roles - user roles
 * </ul>
 * or:
 * <ul>
 * <li>x-turbo-role - user role
 * <li>x-auth-token - JWT token returned from Auth component
 * </ul>
 */
public class HeaderAuthenticationFilter extends OncePerRequestFilter {

    private static final String UNKNOWN = "unknown";
    private final HeaderMapper headerMapper;

    /**
     * Constructor for Spring filter.
     *
     * @param headerMapper Vendor specific header mapper.
     */
    public HeaderAuthenticationFilter(@Nonnull HeaderMapper headerMapper) {
        this.headerMapper = Objects.requireNonNull(headerMapper);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {

        final Optional<String> userName = Optional.ofNullable(
                request.getHeader(headerMapper.getUserName()));
        final Optional<String> group = Optional.ofNullable(headerMapper.getAuthRole(
                request.getHeader(headerMapper.getRole())));
        final String remoteIpAddress = ApiUtils.getClientIp(request).orElse(UNKNOWN);

        final Optional<String> role = Optional.ofNullable(request.getHeader(X_TURBO_ROLE));
        final Optional<String> jwtToken = Optional.ofNullable(request.getHeader(X_TURBO_TOKEN));

        jwtToken.map(jwt -> Optional.of(
                HeaderAuthenticationToken.newBuilder(jwt, remoteIpAddress).build()))
                .orElseGet(() -> userName.flatMap(u -> group.flatMap(g -> Optional.of(
                        HeaderAuthenticationToken.newBuilder(u, g, remoteIpAddress)
                                .setRole(role)
                                .build()))))
                .ifPresent(auth -> SecurityContextHolder.getContext().setAuthentication(auth));
        filterChain.doFilter(request, response);
    }
}