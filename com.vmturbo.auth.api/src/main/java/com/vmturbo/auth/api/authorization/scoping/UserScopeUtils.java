package com.vmturbo.auth.api.authorization.scoping;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.commons.collections4.CollectionUtils;

import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * Static utility methods for finding user scope info from the current security / grpc context
 */
public class UserScopeUtils {

    public static boolean isUserScoped() {
        // first check if there is a security context user
        Optional<AuthUserDTO> user = getAuthUser();
        if (user.isPresent()) {
            return (user.get().getScopeGroups().size() > 0);
        }
        // if no user, check if there is something in the grpc context.
        List<Long> grpcContextScopeGroups = SecurityConstant.USER_SCOPE_GROUPS_KEY.get();
        return (CollectionUtils.isNotEmpty(grpcContextScopeGroups));
    }

    private static Optional<AuthUserDTO> getAuthUser() {
        return SAMLUserUtils.getAuthUserDTO();
    }


    public static List<Long> getUserScopeGroups() {
        // first check if there is a security context user
        Optional<AuthUserDTO> user = getAuthUser();
        if (user.isPresent()) {
            return user.get().getScopeGroups();
        }
        // if no user, check if there is something in the grpc context.
        List<Long> grpcScopeGroups = SecurityConstant.USER_SCOPE_GROUPS_KEY.get();
        return (grpcScopeGroups == null) ? Collections.EMPTY_LIST : grpcScopeGroups;
    }
}
