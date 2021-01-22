package com.vmturbo.api.component.external.api.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.auth.api.authorization.jwt.JwtCallCredential;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Utility functions in support of the XL External API implementation
 **/
public class ApiUtils {
    /**
     * Logger.
     */
    private static final Logger logger = LogManager.getLogger();
    public static final String NOT_IMPLEMENTED_MESSAGE = "REST API message is" +
            " not implemented in Turbonomic XL";
    public static final String LOOPBACK = "127.0.0.1"; // assume it's IPv4 for now
    private static final String X_FORWARDED_FOR = "X-FORWARDED-FOR";

    /**
     * Map Entity types to be expanded to the RelatedEntityType to retrieve. For example,
     * replace requests for stats for a DATACENTER entity with the PHYSICAL_MACHINEs
     * in that DATACENTER.
     */
    private static final Map<ApiEntityType, ApiEntityType> ENTITY_TYPES_TO_EXPAND = ImmutableMap.of(
            ApiEntityType.DATACENTER, ApiEntityType.PHYSICAL_MACHINE
    );

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
     * Check if input scopes contains global scope or not.
     *
     * @param scopes a set of scopes ids.
     * @param groupExpander the group expander
     * @return true if input parameter contains global scope.
     */
    public static boolean containsGlobalScope(@Nonnull final Set<String> scopes,
                                              @Nonnull final GroupExpander groupExpander) {
        if (scopes.isEmpty()) {
            // if there is no specified scopes, it means it is a global scope.
            return true;
        }
        // if the scope list size is larger than default scope size, it will log a warn message.
        final int defaultMaxScopeSize = 50;
        if (scopes.size() >= defaultMaxScopeSize) {
            logger.warn("Search scope list size is too large: {}", scopes.size());
        }
        return scopes.stream()
                .anyMatch(scope -> {
                    if (UuidMapper.isRealtimeMarket(scope)) {
                        return true;
                    }
                    return groupExpander.getGroup(scope)
                            .map(Grouping::getDefinition)
                            .filter(d -> d.getIsTemporary()
                                    && d.hasOptimizationMetadata()
                                    && d.getOptimizationMetadata().getIsGlobalScope())
                            .isPresent();
                });
    }

    /**
     * Check if a scope is a temporary group with global scope. And the entity type of the group
     * matches the input type.
     *
     * @param scopeSet a set of scope IDs that needs to be checked.
     * @param groupExpander the group expander.
     * @param relatedTypes the input related type.
     * @return true if the scope is global temporary group with the same type.
     */
    public static boolean isGlobalTempGroupWithSameEntityType(@Nonnull final Set<String> scopeSet,
                                                              @Nonnull final GroupExpander groupExpander,
                                                              @Nonnull List<String> relatedTypes) {
        final Optional<Integer> globalEntityType = getGlobalTempGroupEntityType(scopeSet, groupExpander);
        return globalEntityType
                .filter(entityType -> relatedTypes.stream()
                        .map(ApiEntityType::fromStringToSdkType)
                        .allMatch(entityType::equals))
                .isPresent();
    }

    /**
     * Check if a scope is a temporary group with global scope. And if temp group entity need to expand,
     * it should use expanded entity type instead of group entity type. If it is a temporary group
     * with global scope, we can speed up query using pre-aggregate market stats table.
     *
     * @param scopeSet a set of scope IDs that needs to be checked.
     * @param groupExpander the group expander.
     * @return return a optional of entity type, if input group is a temporary global scope group,
     * otherwise return empty option.
     */
    public static Optional<Integer> getGlobalTempGroupEntityType(@Nonnull final Set<String> scopeSet,
                                                                 @Nonnull final GroupExpander groupExpander) {
        if (scopeSet.size() != 1) {
            return Optional.empty();
        }
        final Optional<Grouping> groupOptional = groupExpander.getGroup(scopeSet.iterator().next());
        if (!groupOptional.isPresent() || !groupOptional.get().getDefinition().getIsTemporary()
                || !groupOptional.get().getDefinition().hasStaticGroupMembers()
                || groupOptional.get().getDefinition()
                .getStaticGroupMembers().getMembersByTypeCount() != 1) {
            return Optional.empty();
        }

        final GroupDefinition tempGroup = groupOptional.get().getDefinition();
        final boolean isGlobalTempGroup = tempGroup.hasOptimizationMetadata()
                && tempGroup.getOptimizationMetadata().getIsGlobalScope()
                // the global scope optimization.
                && (!tempGroup.getOptimizationMetadata().hasEnvironmentType()
                || tempGroup.getOptimizationMetadata().getEnvironmentType()
                == EnvironmentType.HYBRID);

        int entityType = tempGroup.getStaticGroupMembers()
                .getMembersByType(0)
                .getType()
                .getEntity();

        // if it is global temp group and need to expand, should return target expand entity type.
        if (isGlobalTempGroup && ENTITY_TYPES_TO_EXPAND.containsKey(
                ApiEntityType.fromType(entityType))) {
            return Optional.of(ENTITY_TYPES_TO_EXPAND.get(
                    ApiEntityType.fromType(entityType)).typeNumber());
        } else if (isGlobalTempGroup) {
            // if it is global temp group and not need to expand.
            return Optional.of(entityType);
        } else {
            return Optional.empty();
        }
    }
}


