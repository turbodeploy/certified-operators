package com.vmturbo.auth.component.services;

import javax.annotation.Nonnull;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.vmturbo.auth.component.store.AuthProvider;
import com.vmturbo.components.common.featureflags.FeatureFlags;

/**
 * The AuthServicesController implements the AUTH component Services REST controller.
 */
@RestController
@RequestMapping("/services")
@Api(value = "/services")
public class AuthServicesController {
    /**
     * Auth provider.
     */
    private final AuthProvider authProvider_;

    /**
     * Constructs AuthServicesController.
     *
     * @param authProvider The auth provider.
     */
    public AuthServicesController(final @Nonnull AuthProvider authProvider) {
        authProvider_ = authProvider;
    }

    /**
     * Authorize the service.
     *
     * @return jwtAuthToken signed by auth component.
     * @throws SecurityException if probe security flag is not enabled.
     */
    @ApiOperation(value = "Authorize probe")
    @RequestMapping(path = "authorizeService", method = RequestMethod.POST)
    @ResponseBody
    public @Nonnull String authorizeService()
            throws SecurityException {
        if (!FeatureFlags.ENABLE_TP_PROBE_SECURITY.isEnabled()) {
            throw new SecurityException("Feature not enabled: "
                    + FeatureFlags.ENABLE_TP_PROBE_SECURITY.getName());
        }
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return authProvider_.authorizeService(auth).getCompactRepresentation();
    }
}
