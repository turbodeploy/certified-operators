package com.vmturbo.auth.api.authorization;

import java.util.List;
import java.util.Optional;

import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * Utility methods for accessing user-related information in the current security context.
 */
public class UserContextUtils {
    /**
     * Get the current user id, if one is detected in either the spring security context or GRPC
     * calling context.
     *
     * @return
     */
    static public Optional<String> getCurrentUserId() {
        // first check the spring security context.
        Optional<AuthUserDTO> optionalSecurityContextUser = SAMLUserUtils.getAuthUserDTO();
        if (optionalSecurityContextUser.isPresent()) {
            return Optional.of(optionalSecurityContextUser.get().getUuid());
        }
        // otherwise, check the grpc call context.
        String grpcUserId = SecurityConstant.USER_ID_CTX_KEY.get();
        return grpcUserId == null ? Optional.empty() : Optional.of(grpcUserId);
    }

    /**
     * Get the list of user role names for the current user, if found in the spring security context
     * or GRPC call context.
     *
     * @return
     */
    static public Optional<List<String>> getCurrentUserRoles() {
        // first check if there is a security context user
        final List<String> roles;
        Optional<AuthUserDTO> optionalSecurityContextUser = SAMLUserUtils.getAuthUserDTO();
        if (optionalSecurityContextUser.isPresent()) {
            roles = optionalSecurityContextUser.get().getRoles();
        } else {
            // check grpc context
            roles = SecurityConstant.USER_ROLES_KEY.get();
        }

        return (roles == null ? Optional.empty() : Optional.of(roles));
    }

    /**
     * Does the current user have the specified role?
     *
     * @param roleName
     * @return optional(empty) if the user role information is not present in the calling context.
     * Otherwise, optional(true) if the user definitely has the specified role in their set, and
     * optional(false) if they do not.
     */
    static public Optional<Boolean> currentUserHasRole(String roleName) {
        Optional<List<String>> detectedRoles = getCurrentUserRoles();
        if (detectedRoles.isPresent()) {
            // return true if any roles match the requested string.
            return Optional.of(detectedRoles.get().stream().anyMatch(roleName::equalsIgnoreCase));
        }
        // we don't know
        return Optional.empty();
    }
}
