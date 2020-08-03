package com.vmturbo.api.component.security;

import com.vmturbo.api.component.communication.HeaderAuthenticationToken;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.annotation.Nonnull;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.PublicKey;
import java.util.Objects;
import java.util.Optional;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.X_TURBO_ROLE;

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
    private final Optional<PublicKey> jwtTokenPublicKey;
    private final boolean isLatestVersion;

    /**
     * Constructor for Spring filter.
     *
     * @param headerMapper      Vendor specific header mapper.
     * @param jwtTokenPublicKey public key for JWT token.
     * @param isLatestVersion   is latest version.
     */
    public HeaderAuthenticationFilter(@Nonnull final HeaderMapper headerMapper,
                                      @Nonnull final Optional<String> jwtTokenPublicKey,
                                      boolean isLatestVersion) {
        this.headerMapper = Objects.requireNonNull(headerMapper);
        this.isLatestVersion = isLatestVersion;
        // from injection
        this.jwtTokenPublicKey = isLatestVersion ? headerMapper.buildPublicKeyLatest(jwtTokenPublicKey)
                : headerMapper.buildPublicKey(jwtTokenPublicKey);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        final Optional<String> userName =
                Optional.ofNullable(request.getHeader(headerMapper.getUserName()));
        final Optional<String> group = Optional.ofNullable(
                headerMapper.getAuthGroup(request.getHeader(headerMapper.getRole())));
        final String remoteIpAddress = ApiUtils.getClientIp(request).orElse(UNKNOWN);

        final Optional<String> role = Optional.ofNullable(request.getHeader(X_TURBO_ROLE));
        final Optional<String> jwtToken =
                Optional.ofNullable(request.getHeader(headerMapper.getJwtTokenTag())).map(s -> s.replaceAll("\\s+", ""));
        // from the header
        final Optional<String> header = Optional.ofNullable(request.getHeader(headerMapper.getJwtTokenPublicKeyTag()));
        final Optional<PublicKey> jwtTokenPassedInPublicKey = isLatestVersion ? headerMapper.buildPublicKeyLatest(header)
                : headerMapper.buildPublicKey(header);

        (jwtTokenPublicKey.isPresent() ? jwtTokenPublicKey : jwtTokenPassedInPublicKey).map(
                key -> jwtToken.map(
                        jwt -> HeaderAuthenticationToken.newBuilder(key, jwt, remoteIpAddress, headerMapper)
                                .setIsLatest(isLatestVersion).build()))
                .orElseGet(() -> userName.flatMap(u -> group.flatMap(g -> Optional.of(
                        HeaderAuthenticationToken.newBuilder(u, g, remoteIpAddress)
                                .setRole(role)
                                .build()))))
                .ifPresent(auth -> SecurityContextHolder.getContext().setAuthentication(auth));
        filterChain.doFilter(request, response);
    }
}