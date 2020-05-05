package com.vmturbo.api.component.external.api.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;

import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.auth.api.authorization.jwt.JwtCallCredential;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;

/**
 * Utility functions in support of the XL External API implementation
 **/
public class ApiUtils {

    public static final String NOT_IMPLEMENTED_MESSAGE = "REST API message is" +
            " not implemented in Turbonomic XL";
    public static final String LOOPBACK = "127.0.0.1"; // assume it's IPv4 for now
    private static final String X_FORWARDED_FOR = "X-FORWARDED-FOR";

    public static UnsupportedOperationException notImplementedInXL() {
        return new UnsupportedOperationException(NOT_IMPLEMENTED_MESSAGE);
    }

    /**
     * Generate current authenticated user's JWT CallCredential {@link JwtCallCredential}.
     *
     * @return current user's JWT CallCredential, if user is authenticated.
     */

    public static Optional<JwtCallCredential> generateJWTCallCredential() {
        return getCurrentJWTToken().map(token -> new JwtCallCredential(token));
    }

    /**
     * Get current use's JWT token.
     *
     * @return current user's JWT token if it exists
     */
    public static Optional <String> getCurrentJWTToken() {
        SecurityContext securityContext = SecurityContextHolder.getContext();
        if (securityContext != null
                && securityContext.getAuthentication() != null
                && securityContext.getAuthentication().getPrincipal() instanceof AuthUserDTO) {
            AuthUserDTO authUserDTO = (AuthUserDTO) securityContext.getAuthentication().getPrincipal();
            String jwtToken = authUserDTO.getToken();
            return Optional.of(jwtToken);
        }
        return Optional.empty();
    }

    /**
     * Get logged in user's IP address from new UI.
     * 1. Try to get the originating IP address if client is
     * connecting through a HTTP proxy or load balancer.
     * 2. If originating IP address is not available, try to
     * get IP address of the client or last proxy that sent the request.
     *
     * @param request the HTTP request.
     * @return IP address if remote IP address is available in the request.
     */
    public static Optional<String> getClientIp(HttpServletRequest request) {
        String remoteAddr = null;
        if (request != null) {
            // First try to get the originating IP address if client
            // is connecting through a HTTP proxy or load balancer
            remoteAddr = request.getHeader(X_FORWARDED_FOR);
            // Second if originating IP address is not available
            // try to get IP address of the client or
            // last proxy that sent the request.
            if (remoteAddr == null || remoteAddr.isEmpty()) {
                remoteAddr = request.getRemoteAddr();
            }
        }
        return Optional.ofNullable(remoteAddr);
    }

    /**
     * Get local IP address.
     *
     * @return local IP address if available otherwise fall back to "lookback".
     */
    public static String getLocalIpAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return LOOPBACK;
        }
    }

    /**
     * Check if the input scope is global scope or not.
     *
     * @param scope the scope
     * @param groupOptional a optional of group.
     * @return a boolean to represent if input scope is global market or not.
     */
    public static boolean isGlobalScope(@Nonnull final String scope,
                                        @Nonnull final Optional<Grouping> groupOptional) {
        return Objects.isNull(scope) || UuidMapper.isRealtimeMarket(scope) ||
                (groupOptional.isPresent()
                                && groupOptional.get().getDefinition().getIsTemporary()
                                && groupOptional.get().getDefinition().hasOptimizationMetadata()
                                && groupOptional.get().getDefinition().getOptimizationMetadata()
                                    .getIsGlobalScope());
    }
}


