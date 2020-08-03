package com.vmturbo.auth.api.authorization;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * Utility methods for accessing user-related information in the current security context.
 */
public class UserContextUtils {

    private UserContextUtils() {}

    /**
     * Get the current user id, if one is detected in either the spring security context or GRPC
     * calling context.
     *
     * @return the current user id
     */
    public static Optional<String> getCurrentUserId() {
        return Optional.ofNullable(SAMLUserUtils.getAuthUserDTO()
                .map(AuthUserDTO::getUuid)
                .orElse(SecurityConstant.USER_UUID_KEY.get()));
    }

    /**
     * Get the list of user role names for the current user, if found in the spring security context
     * or GRPC call context.
     *
     * @return the list of user role names
     */
    public static Optional<List<String>> getCurrentUserRoles() {
        return Optional.ofNullable(SAMLUserUtils.getAuthUserDTO()
                .map(AuthUserDTO::getRoles)
                .orElse(SecurityConstant.USER_ROLES_KEY.get()));
    }

    /**
     * Does the current user have the specified role?
     *
     * @param roleName role name
     * @return optional(empty) if the user role information is not present in the calling context.
     * Otherwise, optional(true) if the user definitely has the specified role in their set, and
     * optional(false) if they do not.
     */
    public static Optional<Boolean> currentUserHasRole(String roleName) {
        return getCurrentUserRoles().map(
                strings -> strings.stream().anyMatch(roleName::equalsIgnoreCase));
    }

    /**
     * Get name of current user.
     *
     * @return the current user name or default "SYSTEM" value.
     */
    @Nonnull
    public static String getCurrentUserName() {
        final String userName = SAMLUserUtils.getAuthUserDTO()
                .map(AuthUserDTO::getUser)
                .orElse(SecurityConstant.USER_ID_CTX_KEY.get());
        return userName != null ? userName : "SYSTEM";
    }
}
