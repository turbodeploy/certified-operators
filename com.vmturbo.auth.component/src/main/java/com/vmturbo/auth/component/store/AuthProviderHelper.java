package com.vmturbo.auth.component.store;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.PREDEFINED_ROLE_SET;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.SITE_ADMIN;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.component.store.AuthProvider.UserInfo;

/**
 * Helper class for {@link AuthProvider}.
 */
class AuthProviderHelper {

    private static final String ROLE = "ROLE_";
    private static final String SPRING_SITE_ADMIN_ROLE_TAG = ROLE + SITE_ADMIN;
    private static final String SPRING_ADMINISTRATOR_ROLE_TAG = ROLE + ADMINISTRATOR;

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
                .anyMatch(ga -> SPRING_ADMINISTRATOR_ROLE_TAG.equals(ga.getAuthority()));
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

    /**
     * To change a user's password, the request must either come from the user or from
     * a user with ADMINISTRATOR or SITE_ADMINISTRATOR role. But SITE_ADMINISTRATOR user
     * are not allowed to change ADMINISTRATOR user's password.
     *
     * @param userInfo the user information whose password is about to be changed
     * @return true if the requesting user is allowed to change this user's password
     */
    @VisibleForTesting
    static boolean changePasswordAllowed(@Nonnull final UserInfo userInfo) {
        final Authentication authentication =
                SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) {
            return false;
        }
        return hasRoleAdministrator(authentication) ||
                roleSiteAdminAllowed(authentication, userInfo.isAdminUser()) ||
                SAMLUserUtils.getAuthUserDTO()
                        .map(authUserDTO -> userInfo.userName.equals(authUserDTO.getUser()))
                        .orElse(false);
    }

    // Allow site_admin user changes other (except admin) user password.
    private static boolean roleSiteAdminAllowed(@Nonnull final Authentication authentication, final boolean isAdminUser) {
        return !isAdminUser && authentication.getAuthorities()
                .stream()
                .anyMatch(ga -> SPRING_SITE_ADMIN_ROLE_TAG.equals(ga.getAuthority()));
    }

}
