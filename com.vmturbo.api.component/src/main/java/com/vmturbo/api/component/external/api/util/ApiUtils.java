package com.vmturbo.api.component.external.api.util;

import java.util.Optional;

import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.auth.api.authorization.jwt.JwtCallCredential;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * Utility functions in support of the XL External API implementation
 **/
public class ApiUtils {

    public static final String NOT_IMPLEMENTED_MESSAGE = "REST API message is" +
            " not implemented in Turbonomic XL";

    public static UnsupportedOperationException notImplementedInXL() {
        return new UnsupportedOperationException(NOT_IMPLEMENTED_MESSAGE);
    }

    /**
     * Generate current authenticated user's JWT CallCredential {@link JwtCallCredential}.
     *
     * @return current user's JWT CallCredential, if user is authenticated.
     */

    public static Optional<JwtCallCredential> generateJWTCallCredential() {
        SecurityContext securityContext = SecurityContextHolder.getContext();
        if (securityContext != null
                && securityContext.getAuthentication() != null
                && securityContext.getAuthentication().getPrincipal() != null) {
            AuthUserDTO authUserDTO = (AuthUserDTO) securityContext.getAuthentication().getPrincipal();
            return Optional.of(new JwtCallCredential(authUserDTO.getToken()));
        }
        return Optional.empty();
    }
}
