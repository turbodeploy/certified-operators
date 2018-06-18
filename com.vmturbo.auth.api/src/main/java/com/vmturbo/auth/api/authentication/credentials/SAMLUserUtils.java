package com.vmturbo.auth.api.authentication.credentials;

import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO;


/**
 * SAML user utility class.
 */
@ThreadSafe
public final class SAMLUserUtils {


    /**
     * Get {@link AuthUserDTO} from Spring context
     *
     * @return AuthUserDTO
     */
    public static Optional<AuthUserDTO> getAuthUserDTO() {
        SecurityContext securityContext = SecurityContextHolder.getContext();
        AuthUserDTO authUserDTO = null;
        if (securityContext != null
                && securityContext.getAuthentication() != null
                && securityContext.getAuthentication().getPrincipal() != null) {
            if (securityContext.getAuthentication().getPrincipal() instanceof AuthUserDTO) {
                authUserDTO = (AuthUserDTO) securityContext.getAuthentication().getPrincipal();

            } else if (securityContext.getAuthentication().getDetails() instanceof SAMLUser) {
                SAMLUser samlUser = (SAMLUser) securityContext.getAuthentication().getDetails();
                authUserDTO = samlUser.getAuthUserDTO();
            }
        }
        return Optional.ofNullable(authUserDTO);
    }
}
