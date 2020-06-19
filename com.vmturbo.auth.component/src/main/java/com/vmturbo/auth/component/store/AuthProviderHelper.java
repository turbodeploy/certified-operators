package com.vmturbo.auth.component.store;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.PREDEFINED_ROLE_SET;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Helper class for {@link AuthProvider}.
 */
class AuthProviderHelper {

    // Helper class.
    private AuthProviderHelper() {

    }

    /**
     * Test to see if the requesting user may alter (create/update/delete) a user with the given
     * roles. Only an ADMINISTRATOR may alter other users with ADMINISTRATOR role.
     *
     * @param roleNames the list of roles to be assigned to the new user
     * @return true if the requesting user may create a new user with the given roles
     */
    @VisibleForTesting
    static boolean mayAlterUserWithRoles(@Nonnull final List<String> roleNames) {
        final Authentication authentication =
                SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) {
            return false;
        }
        return (hasRoleAdministrator(authentication) || !roleMatched(roleNames, ADMINISTRATOR));
    }

    /**
     * Does the current user have role ADMINISTRATOR? The security defintions all have the string
     * "ROLE_" prepended to the Turbonomic role.
     *
     * @param authentication authentication from security context.
     * @return true iff the current user has role ADMINISTRATOR
     */
    static boolean hasRoleAdministrator(@Nonnull Authentication authentication) {
        return authentication.getAuthorities()
                .stream()
                .anyMatch(ga -> "ROLE_ADMINISTRATOR".equals(ga.getAuthority()));
    }

    /**
     * Match user roles and intended role case insensitively.
     *
     * @param roles            the list of roles user have
     * @param intendedRoleName the  role name intended to be matched.
     * @return true if one of the provided role match role name case insensitively.
     */
    static boolean roleMatched(final @Nonnull List<String> roles,
            final @Nonnull String intendedRoleName) {
        return roles.stream().anyMatch(role -> role.equalsIgnoreCase(intendedRoleName));
    }

    /**
     * Are valid roles
     *
     * @param roleNames role names
     * @return if all role names are valid and not empty.
     */
    static boolean areValidRoles(@Nonnull final List<String> roleNames) {
        return roleNames.size() > 0 && roleNames.stream()
                .allMatch(name -> PREDEFINED_ROLE_SET.contains(name.toUpperCase()));
    }
}
